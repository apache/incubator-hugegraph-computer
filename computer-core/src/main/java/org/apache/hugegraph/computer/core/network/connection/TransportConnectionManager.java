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

package org.apache.hugegraph.computer.core.network.connection;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.network.ClientFactory;
import org.apache.hugegraph.computer.core.network.ClientHandler;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.MessageHandler;
import org.apache.hugegraph.computer.core.network.TransportClient;
import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.TransportProvider;
import org.apache.hugegraph.computer.core.network.TransportServer;
import org.apache.hugegraph.util.E;

public class TransportConnectionManager implements ConnectionManager {

    private TransportServer server;
    private ClientFactory clientFactory;
    private ClientHandler clientHandler;
    private final ConcurrentHashMap<ConnectionId, TransportClient> clients;

    public TransportConnectionManager() {
        this.clients = new ConcurrentHashMap<>();
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
    public void shutdownServer() {
        if (this.server != null) {
            this.server.shutdown();
            this.server = null;
        }
    }

    @Override
    public synchronized void initClientManager(Config config,
                                               ClientHandler clientHandler) {
        E.checkArgument(this.clientFactory == null,
                        "The clientManager has already been initialized");
        E.checkArgumentNotNull(clientHandler,
                               "The clientHandler parameter can't be null");
        TransportConf conf = TransportConf.wrapConfig(config);
        TransportProvider provider = conf.transportProvider();
        ClientFactory factory = provider.createClientFactory(conf);
        factory.init();
        this.clientFactory = factory;
        this.clientHandler = clientHandler;
    }

    @Override
    public TransportClient getOrCreateClient(ConnectionId connectionId)
                                             throws TransportException {
        E.checkArgument(this.clientFactory != null,
                        "The clientManager has not been initialized yet");
        TransportClient client = this.clients.get(connectionId);
        if (client == null) {
            // Create the client if we don't have it yet.
            ClientFactory clientFactory = this.clientFactory;
            TransportClient newClient = clientFactory.createClient(
                                        connectionId, this.clientHandler);
            this.clients.putIfAbsent(connectionId, newClient);
            client = this.clients.get(connectionId);
        }
        return client;
    }

    @Override
    public TransportClient getOrCreateClient(String host, int port)
                                             throws TransportException {
        E.checkArgument(this.clientFactory != null,
                        "The clientManager has not been initialized yet");
        ConnectionId connectionId = ConnectionId.parseConnectionId(host, port);
        return this.getOrCreateClient(connectionId);
    }

    @Override
    public void closeClient(ConnectionId connectionId) {
        TransportClient client = this.clients.remove(connectionId);
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void shutdownClients() {
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
    public void shutdown() {
        this.shutdownClients();
        this.shutdownServer();
    }
}
