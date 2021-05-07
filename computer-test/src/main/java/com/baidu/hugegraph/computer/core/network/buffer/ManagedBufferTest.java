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

import com.baidu.hugegraph.computer.core.util.StringEncoding;
import com.baidu.hugegraph.testutil.Assert;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

public class ManagedBufferTest {

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
        Assert.assertSame(cnt + 1, nettyManagedBuffer.referenceCount());
        ByteBuf buf = nettyManagedBuffer.nettyByteBuf();
        nettyManagedBuffer.retain();
        Assert.assertSame(cnt + 2, buf.refCnt());
        Assert.assertSame(cnt + 2, nettyManagedBuffer.referenceCount());
        nettyManagedBuffer.release();
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
        Assert.assertSame(cnt - 1, nettyManagedBuffer.referenceCount());
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

    @Test
    @SuppressWarnings("deprecation")
    public void testCopyToByteArray() {
        String testData = "test data";
        byte[] bytesSource = StringEncoding.encode(testData);

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytesSource.length);
        byteBuffer = byteBuffer.put(bytesSource);
        byteBuffer.flip();
        NioManagedBuffer nioManagedBuffer = new NioManagedBuffer(byteBuffer);
        byte[] bytes = nioManagedBuffer.copyToByteArray();
        Assert.assertArrayEquals(bytesSource, bytes);
        Assert.assertNotSame(bytesSource, bytes);

        ByteBuffer byteBuffer2 = ByteBuffer.allocate(bytesSource.length);
        byteBuffer2 = byteBuffer2.put(bytesSource);
        byteBuffer2.flip();
        NioManagedBuffer nioManagedBuffer2 = new NioManagedBuffer(byteBuffer2);
        byte[] bytes2 = nioManagedBuffer2.copyToByteArray();
        Assert.assertArrayEquals(bytesSource, bytes2);
        Assert.assertNotSame(bytesSource, bytes2);

        ByteBuf buf3 = Unpooled.directBuffer(bytesSource.length);
        buf3 = ReferenceCountUtil.releaseLater(buf3);
        buf3 = buf3.writeBytes(bytesSource);
        NettyManagedBuffer nettyManagedBuffer3 = new NettyManagedBuffer(buf3);
        byte[] bytes3 = nettyManagedBuffer3.copyToByteArray();
        Assert.assertArrayEquals(bytesSource, bytes3);
        Assert.assertNotSame(bytesSource, bytes3);

        ByteBuf buf4 = Unpooled.buffer(bytesSource.length);
        buf4 = ReferenceCountUtil.releaseLater(buf4);
        buf4 = buf4.writeBytes(bytesSource);
        NettyManagedBuffer nettyManagedBuffer4 = new NettyManagedBuffer(buf4);
        byte[] bytes4 = nettyManagedBuffer4.copyToByteArray();
        Assert.assertArrayEquals(bytesSource, bytes4);
        Assert.assertNotSame(bytesSource, bytes4);
    }
}
