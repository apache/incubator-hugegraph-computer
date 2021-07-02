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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.util.StringEncoding;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

public class NettyTransportClientTest extends AbstractNetworkTest {

    private static final Logger LOG =
            Log.logger(NettyTransportClientTest.class);

    public static final BarrierEvent BARRIER_EVENT = new BarrierEvent();

    @Override
    protected void initOption() {
        super.updateOption(ComputerOptions.TRANSPORT_MAX_PENDING_REQUESTS,
                           8000);
        super.updateOption(ComputerOptions.TRANSPORT_MIN_PENDING_REQUESTS,
                           6000);
        super.updateOption(ComputerOptions.TRANSPORT_WRITE_BUFFER_HIGH_MARK,
                           64 * (int) Bytes.MB);
        super.updateOption(ComputerOptions.TRANSPORT_WRITE_BUFFER_LOW_MARK,
                           32 * (int) Bytes.MB);
        super.updateOption(ComputerOptions.TRANSPORT_MIN_ACK_INTERVAL,
                           200L);
        super.updateOption(ComputerOptions.TRANSPORT_FINISH_SESSION_TIMEOUT,
                           30_000L);
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
    public void testStartAsync() throws Exception {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        Future<Void> future = client.startSessionAsync();
        future.get(conf.syncRequestTimeout(), TimeUnit.MILLISECONDS);
    }

    @Test
    public void testFinishSession() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        client.startSession();
        client.finishSession();
    }

    @Test
    public void testFinishAsync() throws Exception {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        Future<Void> startFuture = client.startSessionAsync();
        startFuture.get(conf.syncRequestTimeout(), TimeUnit.MILLISECONDS);
        Future<Void> finishFuture = client.finishSessionAsync();
        finishFuture.get(conf.finishSessionTimeout(), TimeUnit.MILLISECONDS);
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
    public void testDataUniformity() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        byte[] sourceBytes = StringEncoding.encode("test data 123");

        Mockito.doAnswer(invocationOnMock -> {
            ManagedBuffer buffer = invocationOnMock.getArgument(2);
            byte[] bytes = buffer.copyToByteArray();
            Assert.assertArrayEquals(sourceBytes, bytes);
            Assert.assertNotSame(sourceBytes, bytes);
            return null;
        }).when(serverHandler).handle(Mockito.eq(MessageType.MSG),
                                      Mockito.eq(1),
                                      Mockito.any());
        client.startSession();
        client.send(MessageType.MSG, 1, ByteBuffer.wrap(sourceBytes));
        client.finishSession();
    }

    @Test
    public void testStartSessionWithTimeout() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        Function<Message, ChannelFuture> sendFunc = message -> null;
        Whitebox.setInternalState(client.clientSession(),
                                  "sendFunction", sendFunc);

        Assert.assertThrows(TransportException.class, () -> {
            client.startSession();
        }, e -> {
            Assert.assertContains("to wait start-response",
                                  e.getMessage());
        });
    }

    @Test
    public void testFinishSessionWithTimeout() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        client.startSession();

        Function<Message, ChannelFuture> sendFunc = message -> null;
        Whitebox.setInternalState(client.clientSession(),
                                  "sendFunction", sendFunc);

        Whitebox.setInternalState(client, "finishSessionTimeout",
                                  1000L);

        Assert.assertThrows(TransportException.class, () -> {
            client.finishSession();
        }, e -> {
            Assert.assertContains("to wait finish-response",
                                  e.getMessage());
        });
    }

    @Test
    public void testStartSessionWithSendException() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        @SuppressWarnings("unchecked")
        Function<Message, ChannelFuture> sendFunc =
                                         Mockito.mock(Function.class);
        Whitebox.setInternalState(client.clientSession(),
                                  "sendFunction", sendFunc);

        Mockito.doThrow(new RuntimeException("test exception"))
               .when(sendFunc)
               .apply(Mockito.any());

        Assert.assertThrows(RuntimeException.class, () -> {
            client.startSession();
        }, e -> {
            Assert.assertContains("test exception", e.getMessage());
        });
    }

    @Test
    public void testFinishSessionWithSendException() throws IOException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        client.startSession();

        @SuppressWarnings("unchecked")
        Function<Message, Future<Void>> sendFunc = Mockito.mock(Function.class);
        Whitebox.setInternalState(client.clientSession(),
                                  "sendFunction", sendFunc);

        Mockito.doThrow(new RuntimeException("test exception"))
               .when(sendFunc)
               .apply(Mockito.any());

        Assert.assertThrows(RuntimeException.class, () -> {
            client.finishSession();
        }, e -> {
            Assert.assertContains("test exception", e.getMessage());
        });
    }

    @Test
    public void testFlowControl() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(
                            StringEncoding.encode("test data"));
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        client.startSession();

        Object sendFuncBak = Whitebox.getInternalState(client.clientSession(),
                                                       "sendFunction");
        Function<Message, ChannelFuture> sendFunc = message -> null;
        Whitebox.setInternalState(client.clientSession(),
                                  "sendFunction", sendFunc);

        for (int i = 1; i <= conf.maxPendingRequests() * 2; i++) {
            boolean send = client.send(MessageType.MSG, 1, buffer);
            if (i <= conf.maxPendingRequests()) {
                Assert.assertTrue(send);
            } else {
                Assert.assertFalse(send);
            }
        }

        int maxRequestId = Whitebox.getInternalState(client.clientSession(),
                                                     "maxRequestId");
        int maxAckId = Whitebox.getInternalState(client.clientSession(),
                                                 "maxAckId");
        Assert.assertEquals(conf.maxPendingRequests(), maxRequestId);
        Assert.assertEquals(0, maxAckId);

        int pendings = conf.maxPendingRequests() - conf.minPendingRequests();
        for (int i = 1; i <= pendings + 1; i++) {
            Assert.assertFalse(client.checkSendAvailable());
            client.clientSession().onRecvAck(i);
        }
        Assert.assertTrue(client.checkSendAvailable());

        maxAckId = Whitebox.getInternalState(client.clientSession(),
                                             "maxAckId");
        Assert.assertEquals(pendings + 1, maxAckId);

        Whitebox.setInternalState(client.clientSession(), "sendFunction",
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

        Whitebox.setInternalState(client, "finishSessionTimeout",
                                  1000L);

        Assert.assertThrows(TransportException.class, () -> {
            client.finishSession();
        }, e -> {
            Assert.assertContains("to wait finish-response",
                                  e.getMessage());
        });

        Mockito.verify(serverHandler, Mockito.timeout(10_000L).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());

        Mockito.verify(clientHandler, Mockito.timeout(10_000L).times(1))
               .exceptionCaught(Mockito.any(), Mockito.any());
    }

    @Test
    public void testTransportPerformance() throws IOException,
                                                  InterruptedException {
        Configurator.setAllLevels("com.baidu.hugegraph", Level.INFO);
        Configurator.setAllLevels("com.baidu.hugegraph.computer.core.network",
                                  Level.WARN);

        NettyTransportClient client = (NettyTransportClient) this.oneClient();
        ByteBuffer buffer = ByteBuffer.allocateDirect(50 * 1024);

        AtomicInteger handledCnt = new AtomicInteger(0);

        Mockito.doAnswer(invocationOnMock -> {
            invocationOnMock.callRealMethod();
            BARRIER_EVENT.signalAll();
            return null;
        }).when(clientHandler).sendAvailable(Mockito.any());

        Mockito.doAnswer(invocationOnMock -> {
            invocationOnMock.callRealMethod();
            handledCnt.getAndIncrement();
            return null;
        }).when(serverHandler).handle(Mockito.any(), Mockito.anyInt(),
                                      Mockito.any());

        long preTransport = System.nanoTime();

        client.startSession();

        int dataNum = 209716;
        long timout = 10_000L;
        for (int i = 0; i < dataNum; i++) {
            boolean send = client.send(MessageType.MSG, 1, buffer);
            if (!send) {
                LOG.info("Current send unavailable");
                i--;
                if (!BARRIER_EVENT.await(timout)) {
                    throw new ComputerException("Timeout(%sms) to wait " +
                                                "sendable", timout);
                }
                BARRIER_EVENT.reset();
            }
        }

        client.finishSession();

        long postTransport = System.nanoTime();

        LOG.info("Transport {} data packets total 10GB, cost {}ms", dataNum,
                 (postTransport - preTransport) / 1000_000L);

        Assert.assertEquals(dataNum, handledCnt.get());

        Configurator.setAllLevels("com.baidu.hugegraph.computer.core.network",
                                  Level.INFO);
    }

    @Test
    public void testCheckMinPendingRequests() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.TRANSPORT_MAX_PENDING_REQUESTS, "100",
                ComputerOptions.TRANSPORT_MIN_PENDING_REQUESTS, "101"
        );
        config = ComputerContext.instance().config();

        TransportConf conf = TransportConf.wrapConfig(config);

        Assert.assertThrows(IllegalArgumentException.class,
                            conf::minPendingRequests);
    }

    @Test
    public void testSessionActive() throws IOException, InterruptedException,
                                           ExecutionException,
                                           TimeoutException {
        NettyTransportClient client = (NettyTransportClient) this.oneClient();

        Assert.assertFalse(client.sessionActive());

        CompletableFuture<Void> future = client.startSessionAsync();
        Assert.assertFalse(client.sessionActive());

        future.get(5, TimeUnit.SECONDS);
        Assert.assertTrue(client.sessionActive());

        CompletableFuture<Void> finishFuture = client.finishSessionAsync();
        Assert.assertTrue(client.sessionActive());

        finishFuture.get(5, TimeUnit.SECONDS);
        Assert.assertFalse(client.sessionActive());

        client.close();
        Assert.assertFalse(client.sessionActive());
    }
}
