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

package org.apache.hugegraph.computer.core.network.connection;

import java.io.IOException;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.network.ClientHandler;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.MessageHandler;
import org.apache.hugegraph.computer.core.network.MockClientHandler;
import org.apache.hugegraph.computer.core.network.MockMessageHandler;
import org.apache.hugegraph.computer.core.network.TransportClient;
import org.apache.hugegraph.computer.core.network.TransportServer;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConnectionManagerTest extends UnitTestBase {

    private static ConnectionManager connectionManager;
    private static int port;

    @Before
    public void setup() {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.TRANSPORT_SERVER_HOST, "127.0.0.1",
                ComputerOptions.TRANSPORT_IO_MODE, "NIO"
        );
        MessageHandler serverHandler = new MockMessageHandler();
        ClientHandler clientHandler = new MockClientHandler();
        connectionManager = new TransportConnectionManager();
        port = connectionManager.startServer(config, serverHandler);
        connectionManager.initClientManager(config, clientHandler);
    }

    @After
    public void teardown() {
        if (connectionManager != null) {
            connectionManager.shutdown();
        }
    }

    @Test
    public void testGetServer() {
        TransportServer server = connectionManager.getServer();
        Assert.assertTrue(server.bound());
        Assert.assertEquals(port, server.port());
        Assert.assertNotEquals(0, server.port());
    }

    @Test
    public void testGetServerWithNoStart() {
        ConnectionManager connectionManager1 = new TransportConnectionManager();
        Assert.assertThrows(IllegalArgumentException.class,
                            connectionManager1::getServer, e -> {
            Assert.assertContains("has not been initialized yet",
                                  e.getMessage());
        });
    }

    @Test
    public void testGetOrCreateClient() throws IOException {
        ConnectionId connectionId = ConnectionId.parseConnectionId(
                                    "127.0.0.1", port);
        TransportClient client = connectionManager.getOrCreateClient(
                                 connectionId);
        Assert.assertTrue(client.active());
    }


    @Test
    public void testCloseClientWithHostAndPort() throws IOException {
        TransportClient client = connectionManager.getOrCreateClient(
                                 "127.0.0.1", port);
        Assert.assertTrue(client.active());
    }

    @Test
    public void testCloseClient() throws IOException {
        ConnectionId connectionId = ConnectionId.parseConnectionId(
                                    "127.0.0.1", port);
        TransportClient client = connectionManager.getOrCreateClient(
                                 connectionId);
        Assert.assertTrue(client.active());
        connectionManager.closeClient(client.connectionId());
        Assert.assertFalse(client.active());
    }

    @Test
    public void testGetClientWithNoInit() {
        ConnectionManager connectionManager1 = new TransportConnectionManager();
        ConnectionId connectionId = ConnectionId.parseConnectionId(
                                    "127.0.0.1", port);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            connectionManager1.getOrCreateClient(connectionId);
        }, e -> {
            Assert.assertContains("has not been initialized yet",
                                  e.getMessage());
        });
    }

    @Test
    public void testShutDown() throws IOException {
        ConnectionId connectionId = ConnectionId.parseConnectionId(
                                    "127.0.0.1", port);
        TransportClient client = connectionManager.getOrCreateClient(
                                 connectionId);
        Assert.assertTrue(client.active());
        connectionManager.shutdownClients();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            connectionManager.getOrCreateClient(connectionId);
        }, e -> {
            Assert.assertContains("has not been initialized yet",
                                  e.getMessage());
        });
        connectionManager.shutdownServer();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            connectionManager.getServer();
        }, e -> {
            Assert.assertContains("has not been initialized yet",
                                  e.getMessage());
        });
        connectionManager.shutdown();
    }
}
