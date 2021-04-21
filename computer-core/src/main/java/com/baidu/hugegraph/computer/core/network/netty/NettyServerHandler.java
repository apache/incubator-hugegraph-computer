/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.computer.core.network.netty;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.message.AbstractMessage;
import com.baidu.hugegraph.computer.core.network.message.AckMessage;
import com.baidu.hugegraph.computer.core.network.message.DataMessage;
import com.baidu.hugegraph.computer.core.network.message.FailMessage;
import com.baidu.hugegraph.computer.core.network.message.FinishMessage;
import com.baidu.hugegraph.computer.core.network.message.StartMessage;
import com.baidu.hugegraph.computer.core.network.session.ServerSession;
import com.baidu.hugegraph.util.Log;
import com.google.common.base.Throwables;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.ScheduledFuture;

public class NettyServerHandler extends AbstractNettyHandler {

    private static final Logger LOG = Log.logger(NettyServerHandler.class);

    private static final long INITIAL_DELAY = 0L;
    private static final AttributeKey<ScheduledFuture<?>> RESPOND_ACK_TASK_KEY =
            AttributeKey.valueOf("RESPOND_ACK_TASK_KEY");

    private final MessageHandler handler;
    private final ServerSession serverSession;
    private final ChannelFutureListenerOnWrite listenerOnWrite;

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
        this.serverSession.startRecv();
        this.respondStartAck(ctx);

        // Add an Schedule task to check respond ack avoid client deadlock wait
        ScheduledFuture<?> respondAckTask = channel.eventLoop()
                                                   .scheduleAtFixedRate(
                        () -> this.checkAndRespondAck(ctx),
                        INITIAL_DELAY, this.serverSession.minAckInterval(),
                        TimeUnit.MILLISECONDS);
        channel.attr(RESPOND_ACK_TASK_KEY).set(respondAckTask);
    }

    @Override
    protected void processFinishMessage(ChannelHandlerContext ctx,
                                        Channel channel,
                                        FinishMessage finishMessage) {
        int finishId = finishMessage.requestId();
        boolean readyFinish = this.serverSession.finishRecv(finishId);
        if (readyFinish) {
            this.respondFinishAck(ctx, finishId);
        }
    }

    @Override
    protected void processDataMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      DataMessage dataMessage) {
        int requestId = dataMessage.requestId();
        this.serverSession.dataRecv(requestId);

        this.handler.handle(dataMessage.type(), dataMessage.partition(),
                            dataMessage.body());

        this.serverSession.handledData(requestId);
    }

    private void respondStartAck(ChannelHandlerContext ctx) {
        AckMessage startAck = new AckMessage(AbstractMessage.START_SEQ);
        ctx.writeAndFlush(startAck).addListener(this.listenerOnWrite);
        this.serverSession.startComplete();
    }

    private void respondFinishAck(ChannelHandlerContext ctx,
                                  int finishId) {
        AckMessage finishAck = new AckMessage(finishId);
        ctx.writeAndFlush(finishAck).addListener(this.listenerOnWrite);
        this.serverSession.finishComplete();

        // Remove the respond ack task
        if (ctx.channel().hasAttr(RESPOND_ACK_TASK_KEY)) {
            ScheduledFuture<?> respondAckTask = ctx.channel()
                                                    .attr(RESPOND_ACK_TASK_KEY)
                                                    .get();
            if (respondAckTask != null) {
                respondAckTask.cancel(false);
            }
            ctx.channel().attr(RESPOND_ACK_TASK_KEY).set(null);
        }
    }

    private void respondDataAck(ChannelHandlerContext ctx, int ackId) {
        AckMessage ackMessage = new AckMessage(ackId);
        ctx.writeAndFlush(ackMessage).addListener(this.listenerOnWrite);
        this.serverSession.respondedDataAck(ackId);
    }

    @Override
    protected void respondFail(ChannelHandlerContext ctx, int failId,
                               int errorCode, String message) {
        FailMessage failMessage = new FailMessage(failId, errorCode, message);
        long timeout = this.serverSession.conf().writeSocketTimeout();
        ctx.writeAndFlush(failMessage).awaitUninterruptibly(timeout);

        if (failId > AbstractMessage.START_SEQ) {
            this.serverSession.handledData(failId);
            this.serverSession.respondedDataAck(failId);
        }
    }

    private void checkAndRespondAck(ChannelHandlerContext ctx) {
        if (this.serverSession.checkFinishReady()) {
            this.respondFinishAck(ctx, this.serverSession.finishId());
        } else if (this.serverSession.checkRespondDataAck()) {
            this.respondDataAck(ctx, this.serverSession.maxHandledId());
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.handler.channelActive(connectionId);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.handler.channelInactive(connectionId);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
                                Throwable cause) throws Exception {
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

        // Respond fail message to requester
        this.respondFail(ctx, AbstractMessage.UNKNOWN_SEQ,
                         exception.errorCode(),
                         Throwables.getStackTraceAsString(exception));

        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.handler.exceptionCaught(exception, connectionId);
    }

    @Override
    protected MessageHandler transportHandler() {
        return this.handler;
    }
}
