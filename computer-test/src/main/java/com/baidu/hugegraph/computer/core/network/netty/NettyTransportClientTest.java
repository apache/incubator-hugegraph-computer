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

package com.baidu.hugegraph.computer.core.network.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.testutil.Assert;

import io.netty.channel.Channel;

public class NettyTransportClientTest extends AbstractNetworkTest {

    @Override
    protected void initOption() {
    }

    @Test
    public void testChannel() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        Channel channel = client.channel();
        Assert.assertTrue(channel.isActive());
    }

    @Test
    public void testConnectID() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        ConnectionId connectionId = ConnectionId.parseConnectionID(host, port);
        ConnectionId clientConnectionId = client.connectionId();
        Assert.assertEquals(connectionId, clientConnectionId);
    }

    @Test
    public void testRemoteAddress() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        ConnectionId connectionId = ConnectionId.parseConnectionID(host, port);
        InetSocketAddress address = client.remoteAddress();
        Assert.assertEquals(connectionId.socketAddress(), address);
    }

    @Test
    public void testStartSession() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        client.startSession();
    }

    @Test
    public void testFinishSession() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        client.finishSession();
    }

    @Test
    public void testSend() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        client.send(MessageType.MSG, 1,
                    ByteBuffer.wrap(TransportUtil.encodeString("hello")));
    }
}
