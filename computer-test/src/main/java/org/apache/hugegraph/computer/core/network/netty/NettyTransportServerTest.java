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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.network.IOMode;
import org.apache.hugegraph.computer.core.network.MockMessageHandler;
import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.channel.epoll.Epoll;

public class NettyTransportServerTest extends UnitTestBase {

    private static Config config;
    private static MockMessageHandler messageHandler;
    private NettyTransportServer server;

    @Before
    public void setup() {
        config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.TRANSPORT_SERVER_HOST, "127.0.0.1",
            ComputerOptions.TRANSPORT_SERVER_PORT, "0",
            ComputerOptions.TRANSPORT_SERVER_THREADS, "3",
            ComputerOptions.TRANSPORT_IO_MODE, "NIO",
            ComputerOptions.TRANSPORT_MAX_SYN_BACKLOG, "1024"
        );
        messageHandler = new MockMessageHandler();
        this.server = new NettyTransportServer();
    }

    @After
    public void teardown() {
        if (this.server != null) {
            this.server.shutdown();
        }
    }

    @Test
    public void testConstructor() throws IOException {
        try (NettyTransportServer nettyServer = new NettyTransportServer()) {
            Assert.assertNotEquals(null, nettyServer);
        }
    }

    @Test
    public void testListenWithDefaultPort() {
        int port = this.server.listen(config, messageHandler);

        TransportConf conf = this.server.conf();
        Assert.assertLte(3, conf.serverThreads());
        Assert.assertEquals(IOMode.NIO, conf.ioMode());
        Assert.assertEquals("127.0.0.1",
                            conf.serverAddress().getHostAddress());

        Assert.assertNotEquals(0, this.server.port());
        Assert.assertNotEquals(0, port);
        Assert.assertEquals("127.0.0.1", this.server.ip());
        String hostName = this.server.bindAddress().getHostName();
        Assert.assertEquals("localhost", hostName);
        Assert.assertEquals(port, this.server.port());
    }

    @Test
    public void testListenWithLocalHost() {
        config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.TRANSPORT_SERVER_HOST, "localhost"
        );
        int port = this.server.listen(config, messageHandler);

        TransportConf conf = this.server.conf();
        Assert.assertEquals("localhost", conf.serverAddress().getHostName());

        Assert.assertNotEquals(0, this.server.port());
        Assert.assertNotEquals(0, port);
        Assert.assertEquals("127.0.0.1", this.server.ip());
        Assert.assertEquals(port, this.server.port());
    }

    @Test
    public void testListenWithLocalAddress() throws UnknownHostException {
        InetAddress localHost = InetAddress.getLocalHost();
        String hostName = localHost.getHostName();
        String ip = InetAddress.getLocalHost().getHostAddress();
        config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.TRANSPORT_SERVER_HOST, hostName
        );
        int port = this.server.listen(config, messageHandler);

        TransportConf conf = this.server.conf();
        Assert.assertEquals(hostName, conf.serverAddress().getHostName());

        Assert.assertNotEquals(0, this.server.port());
        Assert.assertNotEquals(0, port);
        Assert.assertEquals(ip, this.server.ip());
        Assert.assertEquals(port, this.server.port());
    }

    @Test
    public void testListenWithLocalIp() {
        List<String> ips = TransportUtil.getLocalIPAddress();
        if (!ips.isEmpty()) {
            String ip = ips.get(0);
            config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.TRANSPORT_SERVER_HOST, ip
            );
            int port = this.server.listen(config, messageHandler);
            Assert.assertNotEquals(0, this.server.port());
            Assert.assertNotEquals(0, port);
            Assert.assertEquals(ip, this.server.ip());
            Assert.assertEquals(port, this.server.port());
        }
    }

    @Test
    public void testListenWithZeroIp() {
        config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.TRANSPORT_SERVER_HOST, "0.0.0.0"
        );
        int port = this.server.listen(config, messageHandler);

        TransportConf conf = this.server.conf();
        Assert.assertEquals("0.0.0.0", conf.serverAddress().getHostAddress());

        Assert.assertNotEquals(0, port);
        Assert.assertNotEquals(0, this.server.port());
        Assert.assertTrue(this.server.bindAddress().getAddress()
                                     .isAnyLocalAddress());
        Assert.assertEquals(port, this.server.port());
    }

    @Test
    public void testListenWithAssignPort() {
        config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.TRANSPORT_SERVER_HOST, "127.0.0.1",
            ComputerOptions.TRANSPORT_SERVER_PORT, "9091"
        );

        int port = this.server.listen(config, messageHandler);

        TransportConf conf = this.server.conf();
        Assert.assertEquals("127.0.0.1",
                            conf.serverAddress().getHostAddress());

        Assert.assertEquals(9091, this.server.port());
        Assert.assertEquals(9091, port);
        String ip = this.server.bindAddress().getAddress().getHostAddress();
        Assert.assertEquals("127.0.0.1", ip);
        Assert.assertEquals(port, this.server.port());
    }

    @Test
    public void testListenWithInvalidHost() {
        config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.TRANSPORT_SERVER_HOST, "abcdefd",
            ComputerOptions.TRANSPORT_SERVER_PORT, "0",
            ComputerOptions.TRANSPORT_SERVER_THREADS, "3",
            ComputerOptions.TRANSPORT_IO_MODE, "NIO"
        );

        Assert.assertThrows(ComputerException.class, () -> {
            this.server.listen(config, messageHandler);
        }, e -> {
            Assert.assertContains("Failed to parse", e.getMessage());
        });
    }

    @Test
    public void testListenWithInvalidPort() {
        config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.TRANSPORT_SERVER_HOST, "127.0.0.1",
            ComputerOptions.TRANSPORT_SERVER_PORT, "67899",
            ComputerOptions.TRANSPORT_SERVER_THREADS, "3",
            ComputerOptions.TRANSPORT_IO_MODE, "NIO"
        );

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.server.listen(config, messageHandler);
        }, e -> {
            Assert.assertContains("port out of range", e.getMessage());
        });
    }

    @Test
    public void testListenTwice() {
        int port = this.server.listen(config, messageHandler);
        Assert.assertNotEquals(0, this.server.port());
        Assert.assertNotEquals(0, port);
        Assert.assertEquals("127.0.0.1", this.server.ip());
        Assert.assertEquals(port, this.server.port());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            this.server.listen(config, messageHandler);
        }, e -> {
            Assert.assertContains("already been listened", e.getMessage());
        });
    }

    @Test
    public void testEpollMode() {
        config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.TRANSPORT_SERVER_HOST, "127.0.0.1",
            ComputerOptions.TRANSPORT_IO_MODE, "EPOLL"
        );

        if (Epoll.isAvailable()) {
            this.server.listen(config, messageHandler);
            Assert.assertEquals(IOMode.EPOLL, this.server.conf().ioMode());
        } else {
            Assert.assertThrows(UnsatisfiedLinkError.class, () -> {
                this.server.listen(config, messageHandler);
            });
        }
    }
}
