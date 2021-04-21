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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputeException;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.util.StringEncoding;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;

public class NettyTransportClientTest extends AbstractNetworkTest {

    private static final Logger LOG =
            Log.logger(NettyTransportClientTest.class);

    public static final BarrierEvent BARRIER_EVENT = new BarrierEvent();

    @Override
    protected void initOption() {
        super.updateOption(ComputerOptions.TRANSPORT_MAX_PENDING_REQUESTS,
                           500);
        super.updateOption(ComputerOptions.TRANSPORT_MIN_PENDING_REQUESTS,
                           300);
        super.updateOption(ComputerOptions.TRANSPORT_WRITE_BUFFER_LOW_MARK,
                           32 * 1024);
        super.updateOption(ComputerOptions.TRANSPORT_WRITE_BUFFER_HIGH_MARK,
                           64 * 1024);
        super.updateOption(ComputerOptions.TRANSPORT_MIN_ACK_INTERVAL,
                           300L);
        super.updateOption(ComputerOptions.TRANSPORT_FINISH_SESSION_TIMEOUT,
                           15_000L);
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
        for (int i = 0; i < 3; i++) {
            client.startSession();
            client.send(MessageType.MSG, 1,
                        ByteBuffer.wrap(StringEncoding.encode("test1")));
            client.send(MessageType.VERTEX, 2,
                        ByteBuffer.wrap(StringEncoding.encode("test2")));
            client.send(MessageType.EDGE, 3,
                        ByteBuffer.wrap(StringEncoding.encode("test3")));
            client.finishSession();
        }
    }

    @Test
    public void testStartSyncTimeout() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        Function<Message, ?> sendFunc = message -> true;
        Whitebox.setInternalState(client.session(), "sendFunction", sendFunc);

        Assert.assertThrows(TransportException.class, () -> {
            client.startSession();
        }, e -> {
            Assert.assertContains("to wait start response",
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
        }, e -> {
            Assert.assertContains("to wait finish response",
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

        for (int i = 1; i <= conf.maxPendingRequests() * 2; i++) {
            boolean send = client.send(MessageType.MSG, 1, buffer);
            if (i <= conf.maxPendingRequests()) {
                Assert.assertTrue(send);
            } else {
                Assert.assertFalse(send);
            }
        }

        int maxRequestId = Whitebox.getInternalState(client.session(),
                                                     "maxRequestId");
        int maxAckId = Whitebox.getInternalState(client.session(),
                                                 "maxAckId");
        Assert.assertEquals(conf.maxPendingRequests(), maxRequestId);
        Assert.assertEquals(0, maxAckId);

        int pendings = conf.maxPendingRequests() - conf.minPendingRequests();
        for (int i = 1; i <= pendings + 1; i++) {
            Assert.assertFalse(client.checkSendAvailable());
            client.session().ackRecv(i);
        }
        Assert.assertTrue(client.checkSendAvailable());

        maxAckId = Whitebox.getInternalState(client.session(),
                                             "maxAckId");
        Assert.assertEquals(pendings + 1, maxAckId);

        Whitebox.setInternalState(client.session(), "sendFunction",
                                  sendFuncBak);
    }

    @Test
    public void testHandlerException() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        client.startSession();

        Mockito.doThrow(new RuntimeException("test exception"))
               .when(serverHandler)
               .handle(Mockito.any(), Mockito.anyInt(), Mockito.any());

        ByteBuffer buffer = ByteBuffer.wrap(
                            StringEncoding.encode("test data"));
        boolean send = client.send(MessageType.MSG, 1, buffer);
        Assert.assertTrue(send);

        Mockito.verify(serverHandler, Mockito.timeout(10_000L).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());

        Mockito.verify(clientHandler, Mockito.timeout(10_000L).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());
    }

    @Test
    public void testTransportPerformance() throws IOException,
                                                  InterruptedException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024 * 10);

        AtomicLong handledCnt = new AtomicLong(0L);

        Mockito.doAnswer(invocationOnMock -> {
            invocationOnMock.callRealMethod();
            handledCnt.getAndIncrement();
            return null;
        }).when(serverHandler).handle(Mockito.any(), Mockito.anyInt(),
                                      Mockito.any());

        Mockito.doAnswer(invocationOnMock -> {
            invocationOnMock.callRealMethod();
            BARRIER_EVENT.signalAll();
            return null;
        }).when(clientHandler).sendAvailable(Mockito.any());

        long preTransport = System.nanoTime();

        client.startSession();

        for (int i = 0; i < 1024; i++) {
            boolean send = client.send(MessageType.MSG, 1, buffer);
            if (!send) {
                LOG.info("Current send unavailable");
                i--;
                if (!BARRIER_EVENT.await(10_000L)) {
                    throw new ComputeException("Timeout(%sms) to wait send");
                }
                BARRIER_EVENT.reset();
            }
        }

        client.finishSession();

        long postTransport = System.nanoTime();

        LOG.info("Transport 1024 data packets total 10GB, cost {}ms",
                 (postTransport - preTransport) / 1000_000L);

        Assert.assertEquals(1024, handledCnt.get());
    }
}
