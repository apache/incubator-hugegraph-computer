/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.computer.core.network.netty;

import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.MessageHandler;
import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.computer.core.network.buffer.FileRegionBuffer;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.network.message.AbstractMessage;
import org.apache.hugegraph.computer.core.network.message.AckMessage;
import org.apache.hugegraph.computer.core.network.message.DataMessage;
import org.apache.hugegraph.computer.core.network.message.FinishMessage;
import org.apache.hugegraph.computer.core.network.message.StartMessage;
import org.apache.hugegraph.computer.core.network.session.ServerSession;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.ScheduledFuture;

public class NettyServerHandler extends AbstractNettyHandler {

    private static final long INITIAL_DELAY = 0L;

    private final MessageHandler handler;
    private final ServerSession serverSession;
    private final ChannelFutureListenerOnWrite listenerOnWrite;
    private ScheduledFuture<?> respondAckTask;

    public NettyServerHandler(ServerSession serverSession,
                              MessageHandler handler) {
        this.serverSession = serverSession;
        this.handler = handler;
        this.listenerOnWrite = new ChannelFutureListenerOnWrite(this.handler);
    }

    @Override
    protected void processStartMessage(ChannelHandlerContext ctx,
                                       Channel channel,
                                       StartMessage startMessage) {
        this.serverSession.onRecvStateStart();
        this.ackStartMessage(ctx);
    }

    @Override
    protected void processFinishMessage(ChannelHandlerContext ctx,
                                        Channel channel,
                                        FinishMessage finishMessage) {
        int finishId = finishMessage.requestId();
        boolean needAckFinish = this.serverSession.onRecvStateFinish(finishId);
        if (needAckFinish) {
            this.ackFinishMessage(ctx, this.serverSession.finishId());
        }
    }

    @Override
    protected void processDataMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      DataMessage dataMessage) {
        NetworkBuffer body = dataMessage.body();
        try {
            int requestId = dataMessage.requestId();
            this.serverSession.onRecvData(requestId);
            if (body instanceof FileRegionBuffer) {
                this.processFileRegionBuffer(ctx, channel, dataMessage,
                                             (FileRegionBuffer) body);
            } else {
                this.handler.handle(dataMessage.type(), dataMessage.partition(),
                                    dataMessage.body());
                this.serverSession.onHandledData(requestId);
            }
        } finally {
            body.release();
        }
    }

    private void processFileRegionBuffer(ChannelHandlerContext ctx,
                                         Channel channel,
                                         DataMessage dataMessage,
                                         FileRegionBuffer fileRegionBuffer) {
        // Optimize Value of max bytes of next read
        TransportUtil.setMaxBytesPerRead(channel, fileRegionBuffer.length());
        String outputPath = this.handler.genOutputPath(dataMessage.type(),
                                                       dataMessage.partition());
        /*
         * Submit zero-copy task to EventLoop, it will be executed next time
         * network data is received.
         */
        ChannelFuture channelFuture = fileRegionBuffer.transformFromChannel(
                                      (SocketChannel) channel, outputPath);

        channelFuture.addListener((ChannelFutureListener) future -> {
            try {
                if (future.isSuccess()) {
                    this.handler.handle(dataMessage.type(),
                                        dataMessage.partition(),
                                        dataMessage.body());
                    this.serverSession.onHandledData(
                            dataMessage.requestId());
                } else {
                    this.exceptionCaught(ctx, future.cause());
                }
                // Reset max bytes next read to length of frame
                TransportUtil.setMaxBytesPerRead(future.channel(),
                                                 AbstractMessage.HEADER_LENGTH);
                future.channel().unsafe().recvBufAllocHandle().reset(
                                 future.channel().config());
                dataMessage.release();
            } catch (Throwable throwable) {
                this.exceptionCaught(ctx, throwable);
            }
        });
    }

    @Override
    protected void processAckMessage(ChannelHandlerContext ctx, Channel channel,
                                     AckMessage ackMessage) {
        throw new UnsupportedOperationException(
              "Server does not support processAckMessage()");
    }

    private void ackStartMessage(ChannelHandlerContext ctx) {
        AckMessage startAck = new AckMessage(AbstractMessage.START_SEQ);
        ctx.writeAndFlush(startAck).addListener(this.listenerOnWrite);
        this.serverSession.completeStateStart();

        Channel channel = ctx.channel();
        this.handler.onStarted(TransportUtil.remoteConnectionId(channel));

        // Add an schedule task to check and respond ack
        if (this.respondAckTask == null) {
            EventLoop eventExecutors = ctx.channel().eventLoop();
            this.respondAckTask = eventExecutors.scheduleWithFixedDelay(
                                  () -> this.checkAndRespondAck(ctx),
                                  INITIAL_DELAY,
                                  this.serverSession.minAckInterval(),
                                  TimeUnit.MILLISECONDS);
        }
    }

    private void ackFinishMessage(ChannelHandlerContext ctx,
                                  int finishId) {
        AckMessage finishAck = new AckMessage(finishId);
        ctx.writeAndFlush(finishAck).addListener(this.listenerOnWrite);
        this.serverSession.completeStateFinish();

        Channel channel = ctx.channel();
        this.handler.onFinished(TransportUtil.remoteConnectionId(channel));

        // Cancel and remove the task to check respond ack
        if (this.respondAckTask != null) {
            this.respondAckTask.cancel(false);
            this.respondAckTask = null;
        }
    }

    private void ackDataMessage(ChannelHandlerContext ctx, int ackId) {
        AckMessage ackMessage = new AckMessage(ackId);
        ctx.writeAndFlush(ackMessage).addListener(this.listenerOnWrite);
        this.serverSession.onDataAckSent(ackId);
    }

    private void checkAndRespondAck(ChannelHandlerContext ctx) {
        if (this.serverSession.needAckFinish()) {
            this.ackFinishMessage(ctx, this.serverSession.finishId());
        } else if (this.serverSession.needAckData()) {
            this.ackDataMessage(ctx, this.serverSession.maxHandledId());
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.handler.onChannelActive(connectionId);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.handler.onChannelInactive(connectionId);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        TransportException exception;
        Channel channel = ctx.channel();
        if (cause instanceof TransportException) {
            exception = (TransportException) cause;
        } else {
            exception = new TransportException(
                        "%s when the server receive data from '%s'",
                        cause, cause.getMessage(),
                        TransportUtil.remoteAddress(channel));
        }

        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.handler.exceptionCaught(exception, connectionId);
    }

    @Override
    protected ServerSession session() {
        return this.serverSession;
    }

    @Override
    protected MessageHandler transportHandler() {
        return this.handler;
    }
}
