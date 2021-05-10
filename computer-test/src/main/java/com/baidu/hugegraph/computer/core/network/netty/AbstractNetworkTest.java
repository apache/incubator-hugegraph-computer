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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.ClientHandler;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.MockClientHandler;
import com.baidu.hugegraph.computer.core.network.MockMessageHandler;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.TransportServer;
import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.connection.ConnectionManager;
import com.baidu.hugegraph.computer.core.network.connection.TransportConnectionManager;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;

import io.netty.bootstrap.ServerBootstrap;

public abstract class AbstractNetworkTest {

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
        Configurator.setAllLevels("com.baidu.hugegraph", Level.DEBUG);
        OPTIONS.put(ComputerOptions.TRANSPORT_SERVER_HOST, host);
        OPTIONS.put(ComputerOptions.TRANSPORT_IO_MODE, "AUTO");
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
        Configurator.setAllLevels("com.baidu.hugegraph", Level.INFO);
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
