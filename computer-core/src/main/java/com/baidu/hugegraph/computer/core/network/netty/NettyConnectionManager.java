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

import static com.baidu.hugegraph.computer.core.network.netty.NettyTransportServer.newNettyTransportServer;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.ConnectionManager;
import com.baidu.hugegraph.computer.core.network.Transport4Client;
import com.baidu.hugegraph.computer.core.network.Transport4Server;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.TransportProtocol;

import io.netty.buffer.ByteBufAllocator;


public class NettyConnectionManager implements ConnectionManager {

    private final TransportConf conf;
    private final TransportClientFactory clientFactory;
    private final ConcurrentHashMap<ConnectionID, Transport4Client>
                  clientPool = new ConcurrentHashMap<>();


    NettyConnectionManager(Config config) {
        this(config, ByteBufAllocatorFactory.createByteBufAllocator());
    }

    NettyConnectionManager(Config config, ByteBufAllocator bufAllocator) {
        this.conf = new TransportConf(config);
        TransportProtocol protocol = new TransportProtocol(this.conf);
        this.clientFactory = new TransportClientFactory(this.conf, bufAllocator,
                                                        protocol);
    }

    @Override
    public void startup() {
        this.clientFactory.init();
    }

    @Override
    public Transport4Client getAndCreateTransport4Client(String host, int port)
                                                         throws IOException {
        ConnectionID connectionID = ConnectionID.parseConnectionID(host, port);
        return this.getAndCreateTransport4Client(connectionID);
    }

    @Override
    public Transport4Client getAndCreateTransport4Client(
            ConnectionID connectionId) throws IOException {
        return this.clientPool.computeIfAbsent(connectionId, k -> {
            TransportClientFactory clientFactory =
                    NettyConnectionManager.this.clientFactory;
            return clientFactory.createClient(connectionId);
        });
    }

    @Override
    public Transport4Server createServer() {
        return newNettyTransportServer();
    }

    @Override
    public void shutdown() throws IOException {
        this.clientFactory.close();
    }
}
