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

package org.apache.hugegraph.computer.core.network.netty;

import java.io.IOException;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.MockClientHandler;
import org.apache.hugegraph.computer.core.network.MockMessageHandler;
import org.apache.hugegraph.computer.core.network.TransportClient;
import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NettyClientFactoryTest extends UnitTestBase {

    private static Config config;
    private static MockClientHandler clientHandler;
    private NettyTransportServer server;
    private TransportClient client;

    @Before
    public void setup() {
        config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.TRANSPORT_SERVER_HOST, "127.0.0.1",
            ComputerOptions.TRANSPORT_SERVER_PORT, "8086",
            ComputerOptions.TRANSPORT_SERVER_THREADS, "3",
            ComputerOptions.TRANSPORT_IO_MODE, "NIO",
            ComputerOptions.TRANSPORT_RECEIVE_BUFFER_SIZE, "128",
            ComputerOptions.TRANSPORT_SEND_BUFFER_SIZE, "128"
        );
        MockMessageHandler serverHandler = new MockMessageHandler();
        clientHandler = new MockClientHandler();
        this.server = new NettyTransportServer();
        this.server.listen(config, serverHandler);
    }

    @After
    public void teardown() {
        if (this.client != null) {
            this.client.close();
        }
        if (this.server != null) {
            this.server.shutdown();
        }
    }

    @Test
    public void testInit() {
        TransportConf conf = TransportConf.wrapConfig(config);
        NettyClientFactory clientFactory = new NettyClientFactory(conf);
        clientFactory.init();
        Object bootstrap = Whitebox.getInternalState(clientFactory,
                                                     "bootstrap");
        Assert.assertNotNull(bootstrap);
    }

    @Test
    public void testCreateClient() throws IOException {
        TransportConf conf = TransportConf.wrapConfig(config);
        NettyClientFactory clientFactory = new NettyClientFactory(conf);
        clientFactory.init();
        ConnectionId connectionId = ConnectionId.parseConnectionId(
                                    "127.0.0.1", 8086);
        this.client = clientFactory.createClient(connectionId, clientHandler);
        Assert.assertTrue(this.client.active());
    }

    @Test
    public void testClose() throws IOException {
        TransportConf conf = TransportConf.wrapConfig(config);
        NettyClientFactory clientFactory = new NettyClientFactory(conf);
        clientFactory.init();
        ConnectionId connectionId = ConnectionId.parseConnectionId(
                                    "127.0.0.1", 8086);
        this.client = clientFactory.createClient(connectionId, clientHandler);
        Assert.assertTrue(this.client.active());
        this.client.close();
        Assert.assertFalse(this.client.active());
    }

    @Test
    public void testCreateClientWithErrorSocket() {
        TransportConf conf = TransportConf.wrapConfig(config);
        NettyClientFactory clientFactory = new NettyClientFactory(conf);
        clientFactory.init();
        ConnectionId connectionId = ConnectionId.parseConnectionId(
                                    "127.0.0.1", 7777);
        Assert.assertThrows(IOException.class, () -> {
            this.client = clientFactory.createClient(connectionId,
                                                     clientHandler);
        }, e -> {
            Assert.assertContains("Failed to create connection",
                                  e.getMessage());
        });
    }

    @Test
    public void testCreateClientWithNoInit() {
        TransportConf conf = TransportConf.wrapConfig(config);
        NettyClientFactory clientFactory = new NettyClientFactory(conf);
        ConnectionId connectionId = ConnectionId.parseConnectionId(
                                    "127.0.0.1", 7777);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.client = clientFactory.createClient(connectionId,
                                                     clientHandler);
        }, e -> {
            Assert.assertContains("has not been initialized yet",
                                  e.getMessage());
        });
    }
}
