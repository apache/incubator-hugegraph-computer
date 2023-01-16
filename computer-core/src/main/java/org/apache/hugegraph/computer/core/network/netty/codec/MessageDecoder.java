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

package org.apache.hugegraph.computer.core.network.netty.codec;

import org.apache.hugegraph.computer.core.common.exception.IllegalArgException;
import org.apache.hugegraph.computer.core.network.message.AckMessage;
import org.apache.hugegraph.computer.core.network.message.DataMessage;
import org.apache.hugegraph.computer.core.network.message.FinishMessage;
import org.apache.hugegraph.computer.core.network.message.Message;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.network.message.PingMessage;
import org.apache.hugegraph.computer.core.network.message.PongMessage;
import org.apache.hugegraph.computer.core.network.message.StartMessage;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Decoder used by the client side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public class MessageDecoder extends ChannelInboundHandlerAdapter {

    public static final MessageDecoder INSTANCE_FILE_REGION =
                        new MessageDecoder(true);
    public static final MessageDecoder INSTANCE_MEMORY_BUFFER =
                        new MessageDecoder(false);

    private final boolean fileRegionMode;

    private MessageDecoder(boolean fileRegionMode) {
        this.fileRegionMode = fileRegionMode;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        Message message;
        try {
            MessageType msgType = MessageType.decode(buf);
            message = this.decode(ctx, msgType, buf);
            if (message == null) {
                return;
            }
        } finally {
            buf.release();
        }

        ctx.fireChannelRead(message);
    }

    private Message decode(ChannelHandlerContext ctx,
                           MessageType msgType,
                           ByteBuf in) {
        if (msgType.category() == MessageType.Category.DATA) {
            // Decode data message
            if (this.fileRegionMode) {
                return DataMessage.parseWithFileRegion(msgType, in);
            } else {
                return DataMessage.parseWithMemoryBuffer(msgType, in);
            }
        }
        switch (msgType) {
            case START:
                return StartMessage.parseFrom(in);
            case ACK:
                return AckMessage.parseFrom(in);
            case FINISH:
                return FinishMessage.parseFrom(in);
            case PING:
                return PingMessage.parseFrom(in);
            case PONG:
                return PongMessage.parseFrom(in);
            default:
                throw new IllegalArgException("Can't decode message type: %s",
                                              msgType);
        }
    }
}
