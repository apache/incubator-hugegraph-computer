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

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ManagerBufferTest {

    @Test
    public void testRetain() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        ManagedBuffer nioManagedBuffer = new NioManagedBuffer(byteBuffer);
        nioManagedBuffer.retain();
        nioManagedBuffer.release();
        Assert.assertSame(nioManagedBuffer.nioByteBuffer().array(),
                          byteBuffer.array());
        nioManagedBuffer.release();

        ByteBuf byteBuf = Unpooled.buffer(10);
        int cnt = byteBuf.refCnt();
        ManagedBuffer nettyManagedBuffer = new NettyManagedBuffer(byteBuf);
        nettyManagedBuffer.retain();
        Assert.assertSame(cnt + 1, byteBuf.refCnt());
        ByteBuf buf = nettyManagedBuffer.nettyByteBuf();
        Assert.assertSame(cnt + 2, buf.refCnt());
        nettyManagedBuffer.release();
        nettyManagedBuffer.release();
    }

    @Test
    public void testRelease() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        ManagedBuffer nioManagedBuffer = new NioManagedBuffer(byteBuffer);
        Assert.assertSame(nioManagedBuffer.nioByteBuffer().array(),
                          byteBuffer.array());
        nioManagedBuffer.release();

        ByteBuf byteBuf = Unpooled.buffer(10);
        int cnt = byteBuf.refCnt();
        ManagedBuffer nettyManagedBuffer = new NettyManagedBuffer(byteBuf);
        nettyManagedBuffer.release();
        Assert.assertSame(cnt - 1, byteBuf.refCnt());
    }

    @Test
    public void testNioByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        ManagedBuffer nioManagedBuffer = new NioManagedBuffer(byteBuffer);
        Assert.assertSame(nioManagedBuffer.nioByteBuffer().array(),
                          byteBuffer.array());
        nioManagedBuffer.release();

        ByteBuf byteBuf = Unpooled.buffer(10);
        ManagedBuffer nettyManagedBuffer = new NettyManagedBuffer(byteBuf);
        ByteBuffer buffer = nettyManagedBuffer.nioByteBuffer();
        Assert.assertSame(buffer.array(), byteBuf.array());
    }

    @Test
    public void testNettyByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        ManagedBuffer nioManagedBuffer = new NioManagedBuffer(byteBuffer);
        Assert.assertSame(nioManagedBuffer.nettyByteBuf().array(),
                          byteBuffer.array());
        nioManagedBuffer.release();

        ByteBuf byteBuf = Unpooled.buffer(10);
        ManagedBuffer nettyManagedBuffer = new NettyManagedBuffer(byteBuf);
        ByteBuf buf = nettyManagedBuffer.nettyByteBuf();
        Assert.assertSame(buf.array(), byteBuf.array());
    }
}
