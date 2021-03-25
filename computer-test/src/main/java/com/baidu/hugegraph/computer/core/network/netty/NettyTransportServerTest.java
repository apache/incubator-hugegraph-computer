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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputeException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.MockMessageHandler;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Log;

public class NettyTransportServerTest {

    private static final Logger LOG =
            Log.logger(NettyTransportServerTest.class);

    private static Config config;
    private static MockMessageHandler messageHandler;
    private NettyTransportServer server;

    @Before
    public void setup() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.TRANSPORT_SERVER_HOST, "127.0.0.1",
                ComputerOptions.TRANSPORT_SERVER_PORT, "0",
                ComputerOptions.TRANSPORT_SERVER_THREADS, "3",
                ComputerOptions.TRANSPORT_IO_MODE, "NIO"
        );
        config = ComputerContext.instance().config();
        messageHandler = new MockMessageHandler();
        this.server = NettyTransportServer.newNettyTransportServer();
    }

    @After
    public void teardown() {
        if (this.server != null) {
            this.server.stop();
        }
    }

    @Test
    public void testConstructor() throws IOException {
        try (NettyTransportServer nettyServer =
                     NettyTransportServer.newNettyTransportServer()) {
            Assert.assertNotEquals(null, nettyServer);
            nettyServer.stop();
        }
    }

    @Test
    public void testListenWithDefaultPort() {
        int port = this.server.listen(config, messageHandler);

        TransportConf conf = this.server.transportConf();
        Assert.assertEquals(3, conf.serverThreads());
        Assert.assertEquals(IOMode.NIO, conf.ioMode());
        Assert.assertEquals("127.0.0.1",
                            conf.serverAddress().getHostAddress());

        Assert.assertNotEquals(0, this.server.port());
        Assert.assertNotEquals(0, port);
        Assert.assertEquals("127.0.0.1", this.server.host());
        Assert.assertEquals(port, this.server.port());
    }

    @Test
    public void testListenWithRandomPort() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.TRANSPORT_SERVER_HOST, "127.0.0.1",
                ComputerOptions.TRANSPORT_SERVER_PORT, "9091",
                ComputerOptions.TRANSPORT_SERVER_THREADS, "3",
                ComputerOptions.TRANSPORT_IO_MODE, "NIO"
        );
        config = ComputerContext.instance().config();

        int port = this.server.listen(config, messageHandler);

        TransportConf conf = this.server.transportConf();
        Assert.assertEquals(3, conf.serverThreads());
        Assert.assertEquals(IOMode.NIO, conf.ioMode());
        Assert.assertEquals("127.0.0.1",
                            conf.serverAddress().getHostAddress());

        Assert.assertEquals(9091, this.server.port());
        Assert.assertEquals(9091, port);
        Assert.assertEquals("127.0.0.1", this.server.host());
        Assert.assertEquals(port, this.server.port());
    }

    @Test
    public void testListenWithInvalidHost() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.TRANSPORT_SERVER_HOST, "abcdefd",
                ComputerOptions.TRANSPORT_SERVER_PORT, "0",
                ComputerOptions.TRANSPORT_SERVER_THREADS, "3",
                ComputerOptions.TRANSPORT_IO_MODE, "NIO"
        );
        config = ComputerContext.instance().config();

        Assert.assertThrows(ComputeException.class, () -> {
            this.server.listen(config, messageHandler);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("Failed to parse"));
        });
    }

    @Test
    public void testListenWithInvalidPort() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.TRANSPORT_SERVER_HOST, "127.0.0.1",
                ComputerOptions.TRANSPORT_SERVER_PORT, "67899",
                ComputerOptions.TRANSPORT_SERVER_THREADS, "3",
                ComputerOptions.TRANSPORT_IO_MODE, "NIO"
        );
        config = ComputerContext.instance().config();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.server.listen(config, messageHandler);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("port out of range"));
        });

    }

    @Test
    public void testListenTwice() {
        int port = this.server.listen(config, messageHandler);
        Assert.assertNotEquals(0, this.server.port());
        Assert.assertNotEquals(0, port);
        Assert.assertEquals("127.0.0.1", this.server.host());
        Assert.assertEquals(port, this.server.port());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.server.listen(config, messageHandler);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("already been listened"));
        });
    }
}
