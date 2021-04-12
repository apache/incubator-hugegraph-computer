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

import static com.baidu.hugegraph.computer.core.network.TransportUtil.remoteAddress;
import static com.baidu.hugegraph.computer.core.network.TransportUtil.remoteConnectionID;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.message.DataMessage;
import com.baidu.hugegraph.computer.core.network.message.FailMessage;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.computer.core.network.session.ServerSession;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.ChannelHandlerContext;

public class NettyServerHandler extends AbstractNettyHandler {

    private static final Logger LOG = Log.logger(NettyServerHandler.class);

    private final MessageHandler handler;
    private final ServerSession serverSession;

    public NettyServerHandler(ServerSession serverSession,
                              MessageHandler handler) {
        this.serverSession = serverSession;
        this.handler = handler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message message)
                                throws Exception {
        if (message instanceof FailMessage) {
            super.processFailMessage(ctx, (FailMessage) message, this.handler);
            return;
        }
        if (message instanceof DataMessage) {
            this.handler.handle(message.type(), message.partition(),
                                message.body());
        }
        // TODO: handle server message
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ConnectionId connectionId = remoteConnectionID(ctx.channel());
        this.handler.channelActive(connectionId);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ConnectionId connectionId = remoteConnectionID(ctx.channel());
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
                        "Exception on server receive data from %s",
                        cause, remoteAddress(ctx.channel()));
        }
        ConnectionId connectionId = remoteConnectionID(ctx.channel());
        this.handler.exceptionCaught(exception, connectionId);
    }
}
