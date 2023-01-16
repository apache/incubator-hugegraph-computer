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

package org.apache.hugegraph.computer.core.network;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class ConnectionIdTest {

    @Test
    public void testParseConnectionID() {
        ConnectionId connectionId1 = ConnectionId.parseConnectionId(
                                     "127.0.0.1", 8080);
        ConnectionId connectionId2 = ConnectionId.parseConnectionId(
                                     "127.0.0.1", 8080);
        Assert.assertSame(connectionId1, connectionId2);
    }

    @Test
    public void testConnectionIDEquals() {
        ConnectionId connectionId1 = ConnectionId.parseConnectionId(
                                     "localhost", 8080);
        InetSocketAddress address = TransportUtil.resolvedSocketAddress(
                                    "127.0.0.1", 8080);
        ConnectionId connectionId2 = new ConnectionId(address);
        Assert.assertEquals(connectionId1, connectionId2);

        ConnectionId connectionId3 = ConnectionId.parseConnectionId(
                                     "127.0.0.1", 8080, 2);
        Assert.assertNotEquals(connectionId1, connectionId3);
    }

    @Test
    public void testConnectionIDUseUnResolved() {
        InetSocketAddress localSocketAddress =
                InetSocketAddress.createUnresolved("127.0.0.1", 8080);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new ConnectionId(localSocketAddress);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("The address must be " +
                                                      "resolved"));
        });
    }

    @Test
    public void testConnectionIDWithLocalAddress() throws UnknownHostException {
        InetAddress localHost = InetAddress.getLocalHost();
        String hostName = localHost.getHostName();
        InetSocketAddress address = TransportUtil.resolvedSocketAddress(
                                    hostName, 8080);
        ConnectionId connectionId = new ConnectionId(address);
        ConnectionId connectionId2 = ConnectionId.parseConnectionId(
                                     hostName, 8080);
        Assert.assertEquals(connectionId, connectionId2);
    }
}
