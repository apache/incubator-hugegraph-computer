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

package org.apache.hugegraph.computer.core.network.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class NioBuffer implements NetworkBuffer {

    private final ByteBuffer buffer;
    private final AtomicInteger referenceCount;

    public NioBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        this.referenceCount = new AtomicInteger(1);
    }

    @Override
    public int length() {
        return this.buffer.remaining();
    }

    @Override
    public NetworkBuffer retain() {
        this.referenceCount.incrementAndGet();
        return this;
    }

    @Override
    public NetworkBuffer release() {
        this.referenceCount.decrementAndGet();
        return this;
    }

    @Override
    public int referenceCount() {
        return this.referenceCount.get();
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
