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

import org.apache.hugegraph.computer.core.util.StringEncodeUtil;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class NetworkBufferTest {

    @Test
    public void testRetain() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        NetworkBuffer nioNetworkBuffer = new NioBuffer(byteBuffer);
        nioNetworkBuffer.retain();
        nioNetworkBuffer.release();
        Assert.assertSame(nioNetworkBuffer.nioByteBuffer().array(),
                          byteBuffer.array());
        nioNetworkBuffer.release();

        ByteBuf byteBuf = Unpooled.buffer(10);
        int cnt = byteBuf.refCnt();
        NetworkBuffer nettyNetworkBuffer = new NettyBuffer(byteBuf);
        nettyNetworkBuffer.retain();
        Assert.assertSame(cnt + 1, byteBuf.refCnt());
        Assert.assertSame(cnt + 1, nettyNetworkBuffer.referenceCount());
        ByteBuf buf = nettyNetworkBuffer.nettyByteBuf();
        nettyNetworkBuffer.retain();
        Assert.assertSame(cnt + 2, buf.refCnt());
        Assert.assertSame(cnt + 2, nettyNetworkBuffer.referenceCount());
        nettyNetworkBuffer.release();
        nettyNetworkBuffer.release();
        nettyNetworkBuffer.release();
    }

    @Test
    public void testRelease() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        NetworkBuffer nioNetworkBuffer = new NioBuffer(byteBuffer);
        Assert.assertSame(nioNetworkBuffer.nioByteBuffer().array(),
                          byteBuffer.array());
        nioNetworkBuffer.release();

        ByteBuf byteBuf = Unpooled.buffer(10);
        int cnt = byteBuf.refCnt();
        NetworkBuffer nettyNetworkBuffer = new NettyBuffer(byteBuf);
        nettyNetworkBuffer.release();
        Assert.assertSame(cnt - 1, nettyNetworkBuffer.referenceCount());
    }

    @Test
    public void testNioByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        NetworkBuffer nioNetworkBuffer = new NioBuffer(byteBuffer);
        Assert.assertSame(nioNetworkBuffer.nioByteBuffer().array(),
                          byteBuffer.array());
        nioNetworkBuffer.release();

        ByteBuf byteBuf = Unpooled.buffer(10);
        NetworkBuffer nettyNetworkBuffer = new NettyBuffer(byteBuf);
        ByteBuffer buffer = nettyNetworkBuffer.nioByteBuffer();
        Assert.assertSame(buffer.array(), byteBuf.array());
    }

    @Test
    public void testNettyByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(10);
        NetworkBuffer nioNetworkBuffer = new NioBuffer(byteBuffer);
        Assert.assertSame(nioNetworkBuffer.nettyByteBuf().array(),
                          byteBuffer.array());
        nioNetworkBuffer.release();
        Assert.assertEquals(0, nioNetworkBuffer.referenceCount());

        ByteBuf byteBuf = Unpooled.buffer(10);
        NetworkBuffer nettyNetworkBuffer = new NettyBuffer(byteBuf);
        ByteBuf buf = nettyNetworkBuffer.nettyByteBuf();
        Assert.assertSame(buf.array(), byteBuf.array());
    }

    @Test
    public void testCopyToByteArray() {
        String testData = "test data";
        byte[] bytesSource = StringEncodeUtil.encode(testData);

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytesSource.length);
        byteBuffer = byteBuffer.put(bytesSource);
        byteBuffer.flip();
        NioBuffer nioManagedBuffer = new NioBuffer(byteBuffer);
        byte[] bytes = nioManagedBuffer.copyToByteArray();
        Assert.assertArrayEquals(bytesSource, bytes);
        Assert.assertNotSame(bytesSource, bytes);

        ByteBuffer byteBuffer2 = ByteBuffer.allocate(bytesSource.length);
        byteBuffer2 = byteBuffer2.put(bytesSource);
        byteBuffer2.flip();

        int position = byteBuffer2.position();
        int remaining = byteBuffer2.remaining();
        NioBuffer nioManagedBuffer2 = new NioBuffer(byteBuffer2);
        byte[] bytes2 = nioManagedBuffer2.copyToByteArray();
        Assert.assertArrayEquals(bytesSource, bytes2);
        Assert.assertNotSame(bytesSource, bytes2);
        Assert.assertEquals(position, byteBuffer2.position());
        Assert.assertEquals(remaining, byteBuffer2.remaining());

        ByteBuf buf3 = Unpooled.directBuffer(bytesSource.length);
        try {
            buf3 = buf3.writeBytes(bytesSource);
            int readerIndex = buf3.readerIndex();
            int readableBytes = buf3.readableBytes();
            NettyBuffer nettyManagedBuffer3 =
                               new NettyBuffer(buf3);
            byte[] bytes3 = nettyManagedBuffer3.copyToByteArray();
            Assert.assertArrayEquals(bytesSource, bytes3);
            Assert.assertNotSame(bytesSource, bytes3);
            Assert.assertEquals(readerIndex, buf3.readerIndex());
            Assert.assertEquals(readableBytes, buf3.readableBytes());
        } finally {
            buf3.release();
        }

        ByteBuf buf4 = Unpooled.buffer(bytesSource.length);
        try {
            buf4 = buf4.writeBytes(bytesSource);
            NettyBuffer nettyManagedBuffer4 =
                               new NettyBuffer(buf4);
            byte[] bytes4 = nettyManagedBuffer4.copyToByteArray();
            Assert.assertArrayEquals(bytesSource, bytes4);
            Assert.assertNotSame(bytesSource, bytes4);
        } finally {
            buf4.release();
        }
    }
}
