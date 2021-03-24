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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.MockMessageHandler;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.util.Log;

public class NettyTransportServerTest {

    private static final Logger LOG =
            Log.logger(NettyTransportServerTest.class);

    private static Config config;
    private static MockMessageHandler messageHandler;
    private NettyTransportServer transport4Server;

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
    }

    @After
    public void tearDown() {
        if (this.transport4Server != null) {
            this.transport4Server.stop();
        }
    }

    @Test
    public void testConstructor() {
        this.transport4Server = NettyTransportServer.newNettyTransportServer();
        Assert.assertNotEquals(null,
                               this.transport4Server.bufAllocator());

    }

    @Test
    public void testListen() {
        try (NettyTransportServer nettyServer =
                     NettyTransportServer.newNettyTransportServer()) {

            int port = nettyServer.listen(config, messageHandler);

            TransportConf conf = nettyServer.transportConf();
            Assert.assertEquals(3, conf.serverThreads());
            Assert.assertEquals(IOMode.NIO, conf.ioMode());
            Assert.assertEquals("127.0.0.1",
                                conf.serverAddress().getHostAddress());

            Assert.assertNotEquals(0, nettyServer.port());
            Assert.assertNotEquals(0, port);
            Assert.assertEquals("127.0.0.1", nettyServer.host());
            Assert.assertEquals(port, nettyServer.port());
            nettyServer.stop();
        } catch (IOException e) {
            LOG.error("IOException should not have been thrown.", e);
        }
    }
}
