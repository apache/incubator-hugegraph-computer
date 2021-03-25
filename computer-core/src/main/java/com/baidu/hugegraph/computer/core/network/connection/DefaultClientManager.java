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

package com.baidu.hugegraph.computer.core.network.connection;

import com.baidu.hugegraph.computer.core.network.ClientFactory;
import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.Transport4Client;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;


public class DefaultClientManager implements ClientManager {

    private final ClientFactory clientFactory;
    private final ConcurrentHashMap<ConnectionID, Transport4Client>
            clientPool = new ConcurrentHashMap<>();

    DefaultClientManager(ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    @Override
    public void startup() {
        this.clientFactory.init();
    }

    @Override
    public Transport4Client getOrCreateTransport4Client(String host, int port) {
        ConnectionID connectionID = ConnectionID.parseConnectionID(host, port);
        return this.getOrCreateTransport4Client(connectionID);
    }

    @Override
    public Transport4Client getOrCreateTransport4Client(
                            ConnectionID connectionID) {
        return this.clientPool.computeIfAbsent(connectionID, k -> {
            DefaultClientManager clientManager = DefaultClientManager.this;
            return clientManager.clientFactory.createClient(connectionID)
                                .bindClientManger(clientManager);
        });
    }

    @Override
    public void removeClient(ConnectionID connectionID) {
        this.clientPool.remove(connectionID);
    }

    @Override
    public void shutdown() throws IOException {
        this.clientFactory.close();
    }
}
