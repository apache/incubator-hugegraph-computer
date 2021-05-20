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

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.message.AbstractMessage;
import com.baidu.hugegraph.computer.core.network.message.AckMessage;
import com.baidu.hugegraph.computer.core.network.message.DataMessage;
import com.baidu.hugegraph.computer.core.network.message.FinishMessage;
import com.baidu.hugegraph.computer.core.network.message.StartMessage;
import com.baidu.hugegraph.computer.core.network.session.ServerSession;
import com.google.common.base.Throwables;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
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
        try {
            int requestId = dataMessage.requestId();

            this.serverSession.onRecvData(requestId);

            this.handler.handle(dataMessage.type(), dataMessage.partition(),
                                dataMessage.body());

            this.serverSession.onHandledData(requestId);
        } finally {
            dataMessage.release();
        }
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

    @Override
    protected void ackFailMessage(ChannelHandlerContext ctx, int failId,
                                  int errorCode, String message) {
        super.ackFailMessage(ctx, failId, errorCode, message);

        if (failId > AbstractMessage.START_SEQ) {
            this.serverSession.onHandledData(failId);
            this.serverSession.onDataAckSent(failId);
        }
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

        // Respond fail message to requester
        this.ackFailMessage(ctx, AbstractMessage.UNKNOWN_SEQ,
                            exception.errorCode(),
                            Throwables.getStackTraceAsString(exception));

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
