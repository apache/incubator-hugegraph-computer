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

import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.network.ClientHandler;
import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.computer.core.network.message.AbstractMessage;
import org.apache.hugegraph.computer.core.network.message.AckMessage;
import org.apache.hugegraph.computer.core.network.message.DataMessage;
import org.apache.hugegraph.computer.core.network.message.FinishMessage;
import org.apache.hugegraph.computer.core.network.message.Message;
import org.apache.hugegraph.computer.core.network.message.StartMessage;
import org.apache.hugegraph.computer.core.network.session.ClientSession;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class NettyClientHandler extends AbstractNettyHandler {

    private final NettyTransportClient client;

    public NettyClientHandler(NettyTransportClient client) {
        this.client = client;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg)
                                throws Exception {
        // Reset the number of client timeout heartbeat
        ctx.channel().attr(HeartbeatHandler.TIMEOUT_HEARTBEAT_COUNT).set(0);

        super.channelRead0(ctx, msg);
    }

    @Override
    protected void processStartMessage(ChannelHandlerContext ctx,
                                       Channel channel,
                                       StartMessage startMessage) {
        throw new UnsupportedOperationException(
              "Client does not support processStartMessage()");
    }

    @Override
    protected void processFinishMessage(ChannelHandlerContext ctx,
                                        Channel channel,
                                        FinishMessage finishMessage) {
        throw new UnsupportedOperationException(
              "Client does not support processFinishMessage()");
    }

    @Override
    protected void processDataMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      DataMessage dataMessage) {
        throw new UnsupportedOperationException(
              "Client does not support processDataMessage()");
    }

    @Override
    protected void processAckMessage(ChannelHandlerContext ctx,
                                     Channel channel, AckMessage ackMessage) {
        int ackId = ackMessage.ackId();
        assert ackId > AbstractMessage.UNKNOWN_SEQ;
        this.session().onRecvAck(ackId);
        this.client.checkAndNotifySendAvailable();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.transportHandler().onChannelInactive(this.client.connectionId());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
                                Throwable cause) {
        TransportException exception;
        if (cause instanceof TransportException) {
            exception = (TransportException) cause;
        } else {
            exception = new TransportException(
                        "%s when the client receive data from '%s'",
                        cause, cause.getMessage(),
                        TransportUtil.remoteAddress(ctx.channel()));
        }

        this.client.clientHandler().exceptionCaught(exception,
                                                    this.client.connectionId());
    }

    @Override
    protected ClientSession session() {
        return this.client.clientSession();
    }

    @Override
    protected ClientHandler transportHandler() {
        return this.client.clientHandler();
    }
}
