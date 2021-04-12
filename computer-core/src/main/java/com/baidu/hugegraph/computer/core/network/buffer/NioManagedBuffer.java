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
import io.netty.buffer.Unpooled;

public class NioManagedBuffer implements ManagedBuffer {

    private final ByteBuffer buffer;

    public NioManagedBuffer(ByteBuffer buffer) {
        this.buffer = buffer.duplicate();
    }

    @Override
    public int length() {
        return this.buffer.remaining();
    }

    @Override
    public ManagedBuffer retain() {
        return this;
    }

    @Override
    public ManagedBuffer release() {
        return this;
    }

    @Override
    public int referenceCount() {
        return -1;
    }

    @Override
    public ByteBuffer nioByteBuffer() {
        return this.buffer.duplicate();
    }

    @Override
    public ByteBuf nettyByteBuf() {
        return Unpooled.wrappedBuffer(this.buffer);
    }

    @Override
    public byte[] copyToByteArray() {
        byte[] bytes = new byte[this.buffer.remaining()];
        this.buffer.duplicate().get(bytes);
        return bytes;
    }
}
