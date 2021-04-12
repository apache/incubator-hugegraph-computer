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

import static com.baidu.hugegraph.computer.core.network.message.AbstractMessage.HEADER_LENGTH;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.util.Log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.PromiseCombiner;

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
                              ByteBufAllocator allocator)
                              throws TransportException {
        ByteBuf bufHeader = null;
        try {
            PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
            bufHeader = allocator.directBuffer(HEADER_LENGTH);
            message.encodeHeader(bufHeader);
            // Reference will be release called write()
            ChannelFuture headerWriteFuture = ctx.write(bufHeader);
            bufHeader = null;
            combiner.add(headerWriteFuture);
            if (message.hasBody()) {
                ByteBuf bodyBuf = message.body().nettyByteBuf();
                message.body().retain();
                // Reference will be release called write()
                combiner.add(ctx.write(bodyBuf));
            }
            combiner.finish(promise);
        } catch (Throwable e) {
            String msg = String.format("Encode message fail, messageType: %s",
                                       message.type());
            LOG.error(msg, e);
            throw new TransportException(msg, e);
        } finally {
            if (bufHeader != null) {
                bufHeader.release();
            }
            // TODO: need to release the out reference?
            // message.release();
        }
    }
}
