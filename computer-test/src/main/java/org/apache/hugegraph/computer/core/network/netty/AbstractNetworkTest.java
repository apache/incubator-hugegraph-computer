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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.network.ClientHandler;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.MessageHandler;
import org.apache.hugegraph.computer.core.network.MockClientHandler;
import org.apache.hugegraph.computer.core.network.MockMessageHandler;
import org.apache.hugegraph.computer.core.network.TransportClient;
import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.TransportServer;
import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.computer.core.network.connection.ConnectionManager;
import org.apache.hugegraph.computer.core.network.connection.TransportConnectionManager;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import io.netty.bootstrap.ServerBootstrap;

public abstract class AbstractNetworkTest extends UnitTestBase {

    private static final Map<ConfigOption<?>, String> OPTIONS = new HashMap<>();
    protected static Config config;
    protected static TransportConf conf;
    protected static MessageHandler serverHandler;
    protected static ClientHandler clientHandler;
    protected static ConnectionManager connectionManager;
    protected static NettyProtocol clientProtocol;
    protected static NettyProtocol serverProtocol;
    protected static String host;
    protected static int port;

    static {
        List<String> localIPAddress = TransportUtil.getLocalIPAddress();
        if (!localIPAddress.isEmpty()) {
            host = localIPAddress.get(0);
        } else {
            host = "127.0.0.1";
        }
    }

    protected abstract void initOption();

    protected void updateOption(ConfigOption<?> key, Object value) {
        OPTIONS.put(key, String.valueOf(value));
    }

    protected TransportClient oneClient() throws IOException {
        ConnectionId connectionId = ConnectionId.parseConnectionId(host, port);
        TransportClient client = connectionManager.getOrCreateClient(
                                 connectionId);
        Assert.assertTrue(client.active());
        return client;
    }

    @Before
    public void setup() {
        Configurator.setAllLevels("org.apache.hugegraph", Level.DEBUG);
        OPTIONS.put(ComputerOptions.TRANSPORT_SERVER_HOST, host);
        OPTIONS.put(ComputerOptions.TRANSPORT_IO_MODE, "AUTO");
        OPTIONS.put(ComputerOptions.TRANSPORT_SERVER_PORT, "0");
        this.initOption();
        Object[] objects = new Object[OPTIONS.size() * 2];
        Set<Map.Entry<ConfigOption<?>, String>> kvs = OPTIONS.entrySet();
        int i = 0;
        for (Map.Entry<ConfigOption<?>, String> kv : kvs) {
            objects[i++] = kv.getKey();
            objects[i++] = kv.getValue();
        }

        config = UnitTestBase.updateWithRequiredOptions(objects);
        conf = TransportConf.wrapConfig(config);
        serverHandler = Mockito.spy(new MockMessageHandler());
        clientHandler = Mockito.spy(new MockClientHandler());
        connectionManager = new TransportConnectionManager();
        port = connectionManager.startServer(config, serverHandler);
        connectionManager.initClientManager(config, clientHandler);

        this.mockSpyProtocol();
    }

    @After
    public void teardown() {
        if (connectionManager != null) {
            connectionManager.shutdown();
            connectionManager = null;
        }
        Configurator.setAllLevels("org.apache.hugegraph", Level.INFO);
    }

    private void mockSpyProtocol() {
        Object clientFactory = Whitebox.getInternalState(connectionManager,
                                                         "clientFactory");
        NettyProtocol protocol2 = Whitebox.getInternalState(clientFactory,
                                                            "protocol");
        clientProtocol = Mockito.spy(protocol2);
        Whitebox.setInternalState(clientFactory, "protocol",
                                  clientProtocol);

        TransportServer sever = connectionManager.getServer();
        ServerBootstrap bootstrap = Whitebox.getInternalState(sever,
                                                              "bootstrap");
        Object channelInitializer = Whitebox.invoke(ServerBootstrap.class,
                                                    "childHandler", bootstrap);
        NettyProtocol protocol = Whitebox.getInternalState(channelInitializer,
                                                           "protocol");
        serverProtocol = Mockito.spy(protocol);
        Whitebox.setInternalState(channelInitializer, "protocol",
                                  serverProtocol);
    }
}
