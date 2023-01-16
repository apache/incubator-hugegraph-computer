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

import org.apache.hugegraph.computer.core.common.exception.IllegalArgException;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.TransportHandler;
import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.computer.core.network.message.AckMessage;
import org.apache.hugegraph.computer.core.network.message.DataMessage;
import org.apache.hugegraph.computer.core.network.message.FailMessage;
import org.apache.hugegraph.computer.core.network.message.FinishMessage;
import org.apache.hugegraph.computer.core.network.message.Message;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.network.message.PingMessage;
import org.apache.hugegraph.computer.core.network.message.PongMessage;
import org.apache.hugegraph.computer.core.network.message.StartMessage;
import org.apache.hugegraph.computer.core.network.session.TransportSession;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public abstract class AbstractNettyHandler
       extends SimpleChannelInboundHandler<Message> {

    private static final Logger LOG = Log.logger(AbstractNettyHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg)
                                throws Exception {
        Channel channel = ctx.channel();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Receive remote message from '{}', message: {}",
                      TransportUtil.remoteAddress(channel), msg);
        }

        MessageType msgType = msg.type();

        if (msgType.category() == MessageType.Category.DATA) {
            this.processDataMessage(ctx, channel, (DataMessage) msg);
            return;
        }

        switch (msgType) {
            case START:
                this.processStartMessage(ctx, channel, (StartMessage) msg);
                break;
            case FAIL:
                this.processFailMessage(ctx, channel, (FailMessage) msg);
                break;
            case ACK:
                this.processAckMessage(ctx, channel, (AckMessage) msg);
                break;
            case FINISH:
                this.processFinishMessage(ctx, channel, (FinishMessage) msg);
                break;
            case PING:
                this.processPingMessage(ctx, channel, (PingMessage) msg);
                break;
            case PONG:
                this.processPongMessage(ctx, channel, (PongMessage) msg);
                break;
            default:
                throw new IllegalArgException("Unknown message type: %s",
                                              msgType);
        }
    }

    protected abstract void processStartMessage(ChannelHandlerContext ctx,
                                                Channel channel,
                                                StartMessage startMessage);

    protected abstract void processFinishMessage(ChannelHandlerContext ctx,
                                                 Channel channel,
                                                 FinishMessage finishMessage);

    protected abstract void processDataMessage(ChannelHandlerContext ctx,
                                               Channel channel,
                                               DataMessage dataMessage);

    protected abstract void processAckMessage(ChannelHandlerContext ctx,
                                              Channel channel,
                                              AckMessage ackMessage);

    protected void processPingMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      PingMessage pingMessage) {
        ctx.writeAndFlush(PongMessage.INSTANCE)
           .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    protected void processPongMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      PongMessage pongMessage) {
        // No need to deal with the pongMessage, it only for keep-alive
    }

    protected void processFailMessage(ChannelHandlerContext ctx,
                                      Channel channel,
                                      FailMessage failMessage) {
        int errorCode = failMessage.errorCode();

        TransportException exception = new TransportException(
                                       errorCode,
                                       "Remote error from '%s', cause: %s",
                                       TransportUtil.remoteAddress(channel),
                                       failMessage.message());

        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.transportHandler().exceptionCaught(exception, connectionId);
    }

    @Deprecated
    protected void ackFailMessage(ChannelHandlerContext ctx, int failId,
                                  int errorCode, String message) {
        long timeout = this.session().conf().writeSocketTimeout();
        FailMessage failMessage = new FailMessage(failId, errorCode, message);
        ctx.writeAndFlush(failMessage).awaitUninterruptibly(timeout);
    }

    protected abstract TransportSession session();

    protected abstract TransportHandler transportHandler();
}
