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
    private final ConcurrentHashMap<ConnectionID, Transport4Client> clients;

    public TransportConnectionManager() {
        this.clients = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized void initClient(Config config) {
        E.checkArgument(this.clientFactory == null,
                        "ClientFactory has already been listened");
        ClientFactory factory = TransporterFactory.createClientFactory(config);
        factory.init();
        this.clientFactory = factory;
    }

    @Override
    public Transport4Client getOrCreateClient(ConnectionID connectionID)
                                              throws IOException {
        Transport4Client client = this.clients.get(connectionID);
        if (client == null) {
            // Create the client if we don't have it yet.
            ClientFactory clientFactory = this.clientFactory;
            this.clients.putIfAbsent(connectionID,
                                     clientFactory.createClient(connectionID));
            client = this.clients.get(connectionID);
        }
        return client;
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
    public void shutdownClient() {
        if (this.clientFactory != null) {
            this.clientFactory.close();
            this.clientFactory = null;
        }

        if (!this.clients.isEmpty()) {
            this.clients.clear();
        }
    }

    @Override
    public void shutdownServer() {
        if (this.server != null) {
            this.server.shutdown();
            this.server = null;
        }
    }

    @Override
    public void shutdown() {
        this.shutdownClient();
        this.shutdownServer();
    }
}
