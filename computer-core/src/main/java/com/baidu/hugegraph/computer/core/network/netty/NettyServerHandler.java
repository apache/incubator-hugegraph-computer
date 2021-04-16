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

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.message.AckMessage;
import com.baidu.hugegraph.computer.core.network.message.DataMessage;
import com.baidu.hugegraph.computer.core.network.message.FinishMessage;
import com.baidu.hugegraph.computer.core.network.message.StartMessage;
import com.baidu.hugegraph.computer.core.network.session.AckType;
import com.baidu.hugegraph.computer.core.network.session.ServerSession;
import com.baidu.hugegraph.computer.core.network.session.TransportSession;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class NettyServerHandler extends AbstractNettyHandler {

    private static final Logger LOG = Log.logger(NettyServerHandler.class);

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
        this.serverSession.receiveStart();
        this.respondStartAckMessage(ctx);
    }

    @Override
    protected void processFinishMessage(ChannelHandlerContext ctx,
                                        Channel channel,
                                        FinishMessage finishMessage) {
        int finishId = finishMessage.requestId();
        boolean readyFinish = this.serverSession.receiveFinish(finishId);
        if (readyFinish) {
            this.respondFinishAckMessage(ctx, finishId);
        }
    }

    @Override
    protected void processDataMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      DataMessage dataMessage) {
        int requestId = dataMessage.requestId();
        this.session().receivedData(requestId);

        this.handler.handle(dataMessage.type(), dataMessage.partition(),
                            dataMessage.body());

        Pair<AckType, Integer> ackTypePair = this.serverSession
                                                 .handledData(requestId);
        AckType ackType = ackTypePair.getKey();
        int ackId = ackTypePair.getValue();

        assert ackType != null;
        assert ackType != AckType.START;

        if (AckType.FINISH == ackType) {
            this.respondFinishAckMessage(ctx, ackId);
        } else if (AckType.DATA == ackType) {
            this.respondDataAckMessage(ctx, ackId);
        }
    }

    private void respondStartAckMessage(ChannelHandlerContext ctx) {
        AckMessage message = new AckMessage(TransportSession.START_REQUEST_ID);
        ctx.writeAndFlush(message).addListener(this.listenerOnWrite);
        this.serverSession.startComplete();
    }

    private void respondFinishAckMessage(ChannelHandlerContext ctx,
                                         int finishId) {
        AckMessage message = new AckMessage(finishId);
        ctx.writeAndFlush(message).addListener(this.listenerOnWrite);
        this.serverSession.finishComplete();
    }

    private void respondDataAckMessage(ChannelHandlerContext ctx, int ackId) {
        AckMessage message = new AckMessage(ackId);
        ctx.writeAndFlush(message).addListener(this.listenerOnWrite);
        this.serverSession.respondedAck(ackId);
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
        if (cause instanceof TransportException) {
            exception = (TransportException) cause;
        } else {
            exception = new TransportException(
                        "%s when the server receive data from '%s'",
                        cause, cause.getMessage(),
                        TransportUtil.remoteAddress(ctx.channel()));
        }
        Channel channel = ctx.channel();
        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.handler.exceptionCaught(exception, connectionId);
    }

    @Override
    protected MessageHandler transportHandler() {
        return this.handler;
    }

    @Override
    protected ServerSession session() {
        return this.serverSession;
    }
}
