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

package com.baidu.hugegraph.computer.core.network.buffer;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class NettyManagedBuffer implements ManagedBuffer {

    private final ByteBuf buf;

    public NettyManagedBuffer(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public int length() {
        return this.buf.readableBytes();
    }

    @Override
    public ManagedBuffer retain() {
        this.buf.retain();
        return this;
    }

    @Override
    public ManagedBuffer release() {
        this.buf.release();
        return this;
    }

    @Override
    public int referenceCount() {
        return this.buf.refCnt();
    }

    /**
     * NOTE: It will trigger copy when this.buf.nioBufferCount > 1
     */
    @Override
    public ByteBuffer nioByteBuffer() {
        return this.buf.nioBuffer();
    }

    @Override
    public ByteBuf nettyByteBuf() {
        return this.buf.duplicate();
    }

    @Override
    public byte[] copyToByteArray() {
        return ByteBufUtil.getBytes(this.buf.duplicate(),
                                    this.buf.readerIndex(),
                                    this.buf.readableBytes(),
                                    true);
    }
}
