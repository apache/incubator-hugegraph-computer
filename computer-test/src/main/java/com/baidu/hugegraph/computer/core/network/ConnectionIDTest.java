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

package com.baidu.hugegraph.computer.core.network;

import java.net.InetSocketAddress;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

public class ConnectionIDTest {

    @Test
    public void testParseConnectionID() {
        ConnectionID connectionID1 = ConnectionID.parseConnectionID(
                "127.0.0.1", 8080);
        ConnectionID connectionID2 = ConnectionID.parseConnectionID(
                "127.0.0.1", 8080);
        Assert.assertSame(connectionID1, connectionID2);
    }

    @Test
    public void testConnectionIDEquals() {
        ConnectionID connectionID1 = ConnectionID.parseConnectionID(
                "localhost", 8080);
        InetSocketAddress localSocketAddress =
                TransportUtil.resolvedAddress("127.0.0.1", 8080);
        ConnectionID connectionID2 = new ConnectionID(localSocketAddress);
        Assert.assertEquals(connectionID1, connectionID2);

        ConnectionID connectionID3 = ConnectionID.parseConnectionID(
                "127.0.0.1", 8080, 2);
        Assert.assertNotEquals(connectionID1, connectionID3);
    }

    @Test
    public void testConnectionIDUseUnResolved() {
        InetSocketAddress localSocketAddress =
                InetSocketAddress.createUnresolved("127.0.0.1", 8080);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            ConnectionID connectionID2 = new ConnectionID(localSocketAddress);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("The address must be " +
                                                      "resolved"));
        });
    }
}
