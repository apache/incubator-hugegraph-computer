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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.util.Log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
 * Encoder used by the server side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 */
@ChannelHandler.Sharable
public class MessageEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger LOG = Log.logger(MessageEncoder.class);

    public static final MessageEncoder INSTANCE = new MessageEncoder();

    private MessageEncoder() {
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object obj,
                      ChannelPromise promise) throws Exception {
        if (obj instanceof Message) {
            Message message = (Message) obj;
            this.writeMessage(ctx, message, promise, ctx.alloc());
        } else {
            ctx.write(obj, promise);
        }
    }

    private void writeMessage(ChannelHandlerContext ctx,
                              Message message, ChannelPromise promise,
                              ByteBufAllocator allocator) {
        int frameLen = FRAME_HEADER_LENGTH + message.bodyLength();
        ByteBuf buf = null;
        try {
            buf = allocator.directBuffer(frameLen);
            message.encode(buf);
            ctx.write(buf, promise);
        } catch (Throwable e) {
            if (buf != null) {
                buf.release();
            }
            LOG.error("Message encode fail, messageType: {}", message.type());
            throw e;
        } finally {
            if (message.bodyLength() > 0) {
                message.body().release();
            }
        }
    }
}
