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

import java.util.List;

import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.computer.core.network.message.AbstractMessage;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultMaxBytesRecvByteBufAllocator;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * The {@link PreciseFrameDecoder} is a frame Decoder that precisely controls
 * the number of the max bytes per read, the decoder should be chosen when
 * receiving data using zero-copy.
 */
public class PreciseFrameDecoder extends ByteToMessageDecoder {

    private static final Logger LOG = Log.logger(PreciseFrameDecoder.class);

    public PreciseFrameDecoder() {
        super.setSingleDecode(true);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        DefaultMaxBytesRecvByteBufAllocator recvByteBufAllocator =
        new DefaultMaxBytesRecvByteBufAllocator(AbstractMessage.HEADER_LENGTH,
                                                AbstractMessage.HEADER_LENGTH);
        ctx.channel().config().setRecvByteBufAllocator(recvByteBufAllocator);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in,
                          List<Object> out) throws Exception {
        Object decoded = this.decode(ctx, in);
        if (decoded != null) {
            out.add(decoded);
        }
    }

    protected ByteBuf decode(ChannelHandlerContext ctx, ByteBuf in) {
        if (in.readableBytes() < AbstractMessage.HEADER_LENGTH) {
            int nextMaxBytesRead =
                AbstractMessage.HEADER_LENGTH - in.readableBytes();
            TransportUtil.setMaxBytesPerRead(ctx.channel(), nextMaxBytesRead);
            return null;
        }

        // reset max bytes next read to length of frame
        TransportUtil.setMaxBytesPerRead(ctx.channel(),
                                         AbstractMessage.HEADER_LENGTH);

        assert in.readableBytes() <= AbstractMessage.HEADER_LENGTH;

        ByteBuf buf = in.readRetainedSlice(AbstractMessage.HEADER_LENGTH);

        int magicNumber = buf.readShort();
        if (magicNumber != AbstractMessage.MAGIC_NUMBER) {
            LOG.warn("Network stream corrupted: received incorrect " +
                     "magic number: {}, remote address: {}",
                     magicNumber, TransportUtil.remoteAddress(ctx.channel()));
            buf.release();
            return null;
        }
        int version = buf.readByte();
        if (version != AbstractMessage.PROTOCOL_VERSION) {
            LOG.warn("Network stream corrupted: received incorrect " +
                     "protocol version: {}, remote address: {}",
                     version, TransportUtil.remoteAddress(ctx.channel()));
            buf.release();
            return null;
        }

        return buf;
    }
}
