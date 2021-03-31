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

import static com.baidu.hugegraph.computer.core.network.message.AbstractMessage.FRAME_HEADER_LENGTH;
import static com.baidu.hugegraph.computer.core.network.message.AbstractMessage.MAGIC_NUMBER;
import static com.baidu.hugegraph.computer.core.network.message.AbstractMessage.PROTOCOL_VERSION;

import java.util.List;

import com.baidu.hugegraph.computer.core.network.message.Message;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

/**
 * Encoder used by the server side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public class MessageEncoder extends MessageToMessageEncoder<Message> {

    public static final MessageEncoder INSTANCE = new MessageEncoder();

    private MessageEncoder() { }

    @Override
    protected void encode(ChannelHandlerContext ctx, Message message,
                          List<Object> outs) throws Exception {
        int frameLen = FRAME_HEADER_LENGTH + message.bodyLength();
        try {
            ByteBuf buf = ctx.alloc().directBuffer(frameLen);
            buf.writeShort(MAGIC_NUMBER);
            buf.writeByte(PROTOCOL_VERSION);
            message.encode(buf);
            outs.add(buf);
        } finally {
            if (message.bodyLength() > 0) {
                message.body().release();
            }
        }
    }
}
