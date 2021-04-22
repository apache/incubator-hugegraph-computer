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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ClientHandler;
import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.message.AbstractMessage;
import com.baidu.hugegraph.computer.core.network.message.AckMessage;
import com.baidu.hugegraph.computer.core.network.message.FailMessage;
import com.baidu.hugegraph.computer.core.network.session.ClientSession;
import com.baidu.hugegraph.util.Log;
import com.google.common.base.Throwables;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class NettyClientHandler extends AbstractNettyHandler {

    private static final Logger LOG = Log.logger(NettyClientHandler.class);

    private final NettyTransportClient client;

    public NettyClientHandler(NettyTransportClient client) {
        this.client = client;
    }

    @Override
    protected void processAckMessage(ChannelHandlerContext ctx,
                                     Channel channel, AckMessage ackMessage) {
        int ackId = ackMessage.ackId();
        assert ackId > AbstractMessage.UNKNOWN_SEQ;
        this.session().ackRecv(ackId);
        this.client.checkAndNoticeSendAvailable();
    }

    @Override
    protected void processFailMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      FailMessage failMessage) {
        int failId = failMessage.ackId();
        if (failId > AbstractMessage.START_SEQ) {
            this.session().ackRecv(failId);
            this.client.checkAndNoticeSendAvailable();
        }

        super.processFailMessage(ctx, channel, failMessage);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.transportHandler().channelInactive(this.client.connectionId());
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
                        "%s when the client receive data from '%s'",
                        cause, cause.getMessage(),
                        TransportUtil.remoteAddress(ctx.channel()));
        }

        // Respond fail message to requester
        this.respondFail(ctx, AbstractMessage.UNKNOWN_SEQ,
                         exception.errorCode(),
                         Throwables.getStackTraceAsString(exception));

        this.client.handler().exceptionCaught(exception,
                                              this.client.connectionId());
    }

    protected ClientSession session() {
        return this.client.session();
    }

    @Override
    protected ClientHandler transportHandler() {
        return this.client.handler();
    }
}
