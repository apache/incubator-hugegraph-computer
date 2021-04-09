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

package com.baidu.hugegraph.computer.core.network;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

import io.netty.channel.Channel;

public class TransportUtilTest {

    @Test
    public void testRemoteAddressWithNull() {
        Channel channel = null;
        String address = TransportUtil.remoteAddress(channel);
        Assert.assertEquals("<unknown remote>", address);
    }

    @Test
    public void testRemoteConnectionIDWithNull() {
        Channel channel = null;
        ConnectionID connectionID = TransportUtil.remoteConnectionID(channel);
        Assert.assertNull(connectionID);
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
    }

    @Test
    public void testEncodeDecodeString() {
        String str = "hello hugegraph";
        byte[] bytes = TransportUtil.encodeString(str);
        String decodeString = TransportUtil.decodeString(bytes);
        Assert.assertEquals(str, decodeString);
    }
}
