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

package com.baidu.hugegraph.computer.core.network.netty.codec;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.IllegalArgException;
import com.baidu.hugegraph.computer.core.network.message.AckMessage;
import com.baidu.hugegraph.computer.core.network.message.DataMessage;
import com.baidu.hugegraph.computer.core.network.message.FailMessage;
import com.baidu.hugegraph.computer.core.network.message.FinishMessage;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.network.message.PingMessage;
import com.baidu.hugegraph.computer.core.network.message.PongMessage;
import com.baidu.hugegraph.computer.core.network.message.StartMessage;
import com.baidu.hugegraph.util.Log;

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

    private static final Logger LOG = Log.logger(MessageDecoder.class);

    public static final MessageDecoder INSTANCE = new MessageDecoder();

    private MessageDecoder() {
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
                            throws Exception {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }

        ByteBuf buf = (ByteBuf) msg;
        try {
            MessageType msgType = MessageType.decode(buf);
            Message decoded = this.decode(msgType, buf);
            ctx.fireChannelRead(decoded);
        } finally {
            buf.release();
        }
    }

    private Message decode(MessageType msgType, ByteBuf in) {
        if (msgType.category() == MessageType.Category.DATA) {
            // Decode data message
            return DataMessage.parseFrom(msgType, in);
        }
        switch (msgType) {
            case START:
                return StartMessage.INSTANCE;
            case FAIL:
                return FailMessage.parseFrom(in);
            case ACK:
                return AckMessage.parseFrom(in);
            case FINISH:
                return FinishMessage.parseFrom(in);
            case PING:
                return PingMessage.INSTANCE;
            case PONG:
                return PongMessage.INSTANCE;
            default:
                throw new IllegalArgException("Can't decode message type: %s",
                                              msgType);
        }
    }
}
