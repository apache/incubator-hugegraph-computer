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

package org.apache.hugegraph.computer.core.network;

import java.net.InetSocketAddress;

import org.apache.hugegraph.computer.core.util.StringEncodeUtil;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;

public class TransportUtilTest {

    @Test
    public void testRemoteAddressWithNull() {
        Channel channel = null;
        String address = TransportUtil.remoteAddress(channel);
        Assert.assertNull(address);

        Channel channel2 = new EmbeddedChannel();
        channel2.close();
        String address2 = TransportUtil.remoteAddress(channel2);
        Assert.assertNull(address2);
    }

    @Test
    public void testRemoteConnectionIDWithNull() {
        Channel channel = null;
        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        Assert.assertNull(connectionId);

        Channel channel2 = new EmbeddedChannel();
        channel2.close();
        ConnectionId connectionId2 = TransportUtil.remoteConnectionId(channel2);
        Assert.assertNull(connectionId2);
    }

    @Test
    public void testResolvedSocketAddress() {
        InetSocketAddress address = TransportUtil.resolvedSocketAddress(
                                    "www.baidu.com", 80);
        Assert.assertFalse(address.isUnresolved());

        InetSocketAddress address2 = TransportUtil.resolvedSocketAddress(
                                     "www.baidu.com", 9797);
        Assert.assertFalse(address2.isUnresolved());

        InetSocketAddress address3 = TransportUtil.resolvedSocketAddress(
                                     "xxxxx", 80);
        Assert.assertTrue(address3.isUnresolved());

        InetSocketAddress address4 = TransportUtil.resolvedSocketAddress(
                                     "127.0.0.1", 80);
        Assert.assertFalse(address4.isUnresolved());
    }

    @Test
    public void testFormatAddress() {
        InetSocketAddress address = TransportUtil.resolvedSocketAddress(
                                    "xxxxx", 80);
        String formatAddress = TransportUtil.formatAddress(address);
        Assert.assertEquals("xxxxx:80", formatAddress);

        InetSocketAddress address2 = TransportUtil.resolvedSocketAddress(
                                     "127.0.0.1", 8089);
        String formatAddress2 = TransportUtil.formatAddress(address2);
        Assert.assertContains("127.0.0.1:8089", formatAddress2);
    }

    @Test
    public void testReadString() {
        byte[] testData = StringEncodeUtil.encode("test data");
        ByteBuf buffer = Unpooled.directBuffer(testData.length);
        try {
            buffer.writeInt(testData.length);
            buffer.writeBytes(testData);
            String readString = TransportUtil.readString(buffer);
            Assert.assertEquals("test data", readString);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testWriteString() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            TransportUtil.writeString(buffer, "test data");
            String readString = TransportUtil.readString(buffer);
            Assert.assertEquals("test data", readString);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testWriteStringWithEmptyString() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            TransportUtil.writeString(buffer, "");
            String readString = TransportUtil.readString(buffer);
            Assert.assertEquals("", readString);
        } finally {
            buffer.release();
        }
    }
}
