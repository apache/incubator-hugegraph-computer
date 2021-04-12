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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.ClientFactory;
import com.baidu.hugegraph.computer.core.network.ClientHandler;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.TransportProvider;
import com.baidu.hugegraph.computer.core.network.TransportServer;
import com.baidu.hugegraph.util.E;

public class TransportConnectionManager implements ConnectionManager {

    private TransportServer server;
    private ClientFactory clientFactory;
    private ClientHandler clientHandler;
    private final ConcurrentHashMap<ConnectionId, TransportClient> clients;

    public TransportConnectionManager() {
        this.clients = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized void initClientManager(Config config,
                                               ClientHandler clientHandler) {
        E.checkArgument(this.clientFactory == null,
                        "The clientManager has already been initialized");
        E.checkArgumentNotNull(clientHandler,
                               "The clientHandler param can't be null");
        TransportConf conf = TransportConf.wrapConfig(config);
        TransportProvider provider = conf.transportProvider();
        ClientFactory factory = provider.createClientFactory(conf);
        factory.init();
        this.clientFactory = factory;
        this.clientHandler = clientHandler;
    }

    @Override
    public TransportClient getOrCreateClient(ConnectionId connectionID)
                                             throws IOException {
        E.checkArgument(this.clientFactory != null,
                        "The clientManager has not been initialized yet");
        TransportClient client = this.clients.get(connectionID);
        if (client == null) {
            // Create the client if we don't have it yet.
            ClientFactory clientFactory = this.clientFactory;
            TransportClient newClient = clientFactory.createClient(
                                        connectionID, this.clientHandler);
            this.clients.putIfAbsent(connectionID, newClient);
            client = this.clients.get(connectionID);
        }
        return client;
    }

    @Override
    public TransportClient getOrCreateClient(String host, int port)
                                             throws IOException {
        E.checkArgument(this.clientFactory != null,
                        "The clientManager has not been initialized yet");
        ConnectionId connectionID = ConnectionId.parseConnectionID(host, port);
        return this.getOrCreateClient(connectionID);
    }

    @Override
    public void closeClient(ConnectionId connectionID) {
        TransportClient client = this.clients.get(connectionID);
        if (client != null) {
            this.clients.remove(connectionID);
            client.close();
        }
    }

    @Override
    public synchronized int startServer(Config config,
                                        MessageHandler serverHandler) {
        E.checkArgument(this.server == null,
                        "The TransportServer has already been listened");
        E.checkArgumentNotNull(serverHandler,
                               "The serverHandler param can't be null");
        TransportConf conf = TransportConf.wrapConfig(config);
        TransportServer server = conf.transportProvider().createServer(conf);
        int bindPort = server.listen(config, serverHandler);
        this.server = server;
        return bindPort;
    }

    @Override
    public TransportServer getServer() {
        E.checkArgument(this.server != null && this.server.bound(),
                        "The TransportServer has not been initialized yet");
        return this.server;
    }

    @Override
    public void shutdownClientManager() {
        if (this.clientFactory != null) {
            this.clientFactory.close();
            this.clientFactory = null;
        }

        if (!this.clients.isEmpty()) {
            Iterator<Map.Entry<ConnectionId, TransportClient>>
            iterator = this.clients.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ConnectionId, TransportClient> next = iterator.next();
                TransportClient client = next.getValue();
                if (client != null) {
                    client.close();
                }
                iterator.remove();
            }
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
        this.shutdownClientManager();
        this.shutdownServer();
    }
}
