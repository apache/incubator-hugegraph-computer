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
import java.util.function.Function;

import org.junit.Test;
import org.mockito.Mockito;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.util.StringEncoding;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;

import io.netty.channel.Channel;

public class NettyTransportClientTest extends AbstractNetworkTest {

    @Override
    protected void initOption() {
        super.updateOption(ComputerOptions.TRANSPORT_MAX_PENDING_REQUESTS, 5);
        super.updateOption(ComputerOptions.TRANSPORT_MIN_PENDING_REQUESTS, 2);
        super.updateOption(ComputerOptions.TRANSPORT_SYNC_REQUEST_TIMEOUT,
                           5_000L);
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
        ConnectionId connectionId = ConnectionId.parseConnectionId(host, port);
        ConnectionId clientConnectionId = client.connectionId();
        Assert.assertEquals(connectionId, clientConnectionId);
    }

    @Test
    public void testRemoteAddress() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        ConnectionId connectionId = ConnectionId.parseConnectionId(host, port);
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
        client.startSession();
        client.finishSession();
    }

    @Test
    public void testSend() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        client.startSession();
        client.send(MessageType.MSG, 1,
                    ByteBuffer.wrap(StringEncoding.encode("test1")));
        client.send(MessageType.VERTEX, 2,
                    ByteBuffer.wrap(StringEncoding.encode("test2")));
        client.send(MessageType.EDGE, 3,
                    ByteBuffer.wrap(StringEncoding.encode("test3")));
        client.finishSession();
    }

    @Test
    public void testStartSyncTimeout() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        Function<Message, ?> sendFunc = message -> true;
        Whitebox.setInternalState(client.session(), "sendFunction", sendFunc);

        Assert.assertThrows(TransportException.class, () -> {
            client.startSession();
            UnitTestBase.sleep(5_000L);
        }, e -> {
            Assert.assertContains("Timeout(5000ms) to wait start response",
                                  e.getMessage());
        });
    }

    @Test
    public void testFinishSyncTimeout() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        client.startSession();

        Function<Message, ?> sendFunc = message -> true;
        Whitebox.setInternalState(client.session(), "sendFunction", sendFunc);

        Assert.assertThrows(TransportException.class, () -> {
            client.finishSession();
            UnitTestBase.sleep(5_000L);
        }, e -> {
            Assert.assertContains("Timeout(5000ms) to wait finish response",
                                  e.getMessage());
        });
    }

    @Test
    public void testFlowController() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(
                            StringEncoding.encode("test data"));
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        client.startSession();

        Object sendFuncBak = Whitebox.getInternalState(client.session(),
                                                       "sendFunction");
        Function<Message, ?> sendFunc = message -> true;
        Whitebox.setInternalState(client.session(), "sendFunction", sendFunc);

        for (int i = 1; i <= 10; i++) {
            boolean send = client.send(MessageType.MSG, 1, buffer);
            if (i <= 5) {
                Assert.assertTrue(send);
            } else {
                Assert.assertFalse(send);
            }
        }

        int maxRequestId = Whitebox.getInternalState(client.session(),
                                                     "maxRequestId");
        int maxAckId = Whitebox.getInternalState(client.session(),
                                                 "maxAckId");
        Assert.assertEquals(5, maxRequestId);
        Assert.assertEquals(0, maxAckId);

        for (int i = 1; i <= 4; i++) {
            Assert.assertFalse(client.checkSendAvailable());
            client.session().ackRecv(i);
        }
        Assert.assertTrue(client.checkSendAvailable());

        maxAckId = Whitebox.getInternalState(client.session(),
                                             "maxAckId");
        Assert.assertEquals(4, maxAckId);

        Whitebox.setInternalState(client.session(), "sendFunction",
                                  sendFuncBak);
    }

    @Test
    public void testHandlerException() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        Mockito.doThrow(new RuntimeException("test exception"))
               .when(serverHandler)
               .handle(Mockito.any(), Mockito.anyInt(), Mockito.any());
        client.startSession();
        ByteBuffer buffer = ByteBuffer.wrap(
                            StringEncoding.encode("test data"));
        boolean send = client.send(MessageType.MSG, 1, buffer);
        Assert.assertTrue(send);

        Mockito.verify(serverHandler, Mockito.timeout(2000L).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());

        Mockito.verify(clientHandler, Mockito.timeout(2000L).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());
    }
}
