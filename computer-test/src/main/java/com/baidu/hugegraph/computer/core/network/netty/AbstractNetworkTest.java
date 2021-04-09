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
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.MockMessageHandler;
import com.baidu.hugegraph.computer.core.network.MockTransportHandler;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.computer.core.network.TransportHandler;
import com.baidu.hugegraph.computer.core.network.TransportServer;
import com.baidu.hugegraph.computer.core.network.connection.ConnectionManager;
import com.baidu.hugegraph.computer.core.network.connection.TransportConnectionManager;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.TimeUtil;

import io.netty.bootstrap.ServerBootstrap;

public abstract class AbstractNetworkTest {

    private static final Map<ConfigOption<?>, String> OPTIONS = new HashMap<>();
    protected static Config config;
    protected static MessageHandler serverHandler;
    protected static TransportHandler clientHandler;
    protected static ConnectionManager connectionManager;
    protected static NettyProtocol clientProtocol;
    protected static NettyProtocol serverProtocol;
    protected static String host = "127.0.0.1";
    protected static int port;

    protected abstract void initOption();

    protected void updateOption(ConfigOption<?> key, Object value) {
        OPTIONS.put(key, String.valueOf(value));
    }

    protected TransportClient oneClient() throws IOException {
        ConnectionID connectionID = ConnectionID.parseConnectionID(host, port);
        TransportClient client = connectionManager.getOrCreateClient(
                                 connectionID);
        Assert.assertTrue(client.active());
        return client;
    }

    @Before
    public void setup() {
        OPTIONS.put(ComputerOptions.TRANSPORT_SERVER_HOST, host);
        OPTIONS.put(ComputerOptions.TRANSPORT_IO_MODE, "NIO");
        this.initOption();
        Object[] objects = new Object[OPTIONS.size() * 2];
        Set<Map.Entry<ConfigOption<?>, String>> kvs = OPTIONS.entrySet();
        int i = 0;
        for (Map.Entry<ConfigOption<?>, String> kv : kvs) {
            objects[i++] = kv.getKey();
            objects[i++] = kv.getValue();
        }

        UnitTestBase.updateWithRequiredOptions(objects);
        config = ComputerContext.instance().config();
        serverHandler = Mockito.spy(new MockMessageHandler());
        clientHandler = Mockito.spy(new MockTransportHandler());
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

    protected static void waitTillNext(long seconds) {
        TimeUtil.tillNextMillis(TimeUtil.timeGen() + seconds * 1000L);
    }
}
