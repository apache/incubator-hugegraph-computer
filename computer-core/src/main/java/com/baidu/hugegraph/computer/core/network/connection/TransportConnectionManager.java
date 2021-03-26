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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.ClientFactory;
import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.Transport4Client;
import com.baidu.hugegraph.computer.core.network.Transport4Server;
import com.baidu.hugegraph.computer.core.network.TransporterFactory;
import com.baidu.hugegraph.util.E;

public class TransportConnectionManager implements ConnectionManager {

    private Transport4Server server;
    private ClientFactory clientFactory;
    private final ConcurrentHashMap<ConnectionID, Transport4Client>
            clients = new ConcurrentHashMap<>();

    @Override
    public synchronized void initClientFactory(Config config) {
        E.checkArgument(this.clientFactory == null,
                        "ClientFactory has already been listened");
        this.clientFactory = TransporterFactory.createClientFactory(config);
    }

    @Override
    public Transport4Client getOrCreateClient(ConnectionID connectionID)
                                              throws IOException {
        Function<ConnectionID, Transport4Client> createClientFunc = k -> {
            ClientFactory clientFactory = this.clientFactory;
            return clientFactory.createClient(connectionID);
        };
        return this.clients.computeIfAbsent(connectionID, createClientFunc);
    }

    @Override
    public Transport4Client getOrCreateClient(String host, int port)
                                              throws IOException {
        ConnectionID connectionID = ConnectionID.parseConnectionID(host, port);
        return this.getOrCreateClient(connectionID);
    }

    @Override
    public void closeClient(Transport4Client client) {
        if (client != null && client.connectionID() != null) {
            boolean ret = this.clients.remove(client.connectionID(), client);
            if (ret) {
                client.close();
            }
        }
    }

    @Override
    public synchronized int startServer(Config config, MessageHandler handler) {
        E.checkArgument(this.server == null,
                        "The server has already been listened");
        Transport4Server server = TransporterFactory.createServer(config);
        assert server != null;
        int bindPort = server.listen(config, handler);
        this.server = server;
        return bindPort;
    }

    @Override
    public Transport4Server getServer() {
        E.checkArgument(this.server != null && this.server.isBound(),
                        "Transport4Server has not been initialized yet");
        return this.server;
    }

    @Override
    public void shutdownClientFactory() throws IOException {
        if (this.clientFactory != null) {
            this.clientFactory.close();
        }
    }

    @Override
    public void shutdownServer() throws IOException {
        if (this.server != null) {
            this.server.shutdown();
        }
    }

    @Override
    public void shutdown() throws IOException {
        this.shutdownClientFactory();
        this.shutdownServer();
    }
}
