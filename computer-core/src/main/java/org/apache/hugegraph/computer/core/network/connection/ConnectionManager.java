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

import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.network.ClientHandler;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.MessageHandler;
import org.apache.hugegraph.computer.core.network.TransportClient;
import org.apache.hugegraph.computer.core.network.TransportServer;

/**
 * This is used for unified manage server and client connection.
 */
public interface ConnectionManager {

    /**
     * Start the server, return the port listened.
     * This method is called only once.
     */
    int startServer(Config config, MessageHandler serverHandler);

    /**
     * Return the only one listened server.
     */
    TransportServer getServer();

    /**
     * Shutdown the server.
     */
    void shutdownServer();

    /**
     * Initialize the client connection manager.
     * This method is called only once.
     */
    void initClientManager(Config config, ClientHandler clientHandler);

    /**
     * Get a {@link TransportClient} instance from the connection pool first.
     * If it is not found or not active, create a new one.
     * @param connectionId {@link ConnectionId}
     */
    TransportClient getOrCreateClient(ConnectionId connectionId)
                                      throws TransportException;

    /**
     * Get a {@link TransportClient} instance from the connection pool first.
     * If {it is not found or not active, create a new one.
     * @param host the hostName or Ip
     * @param port the port
     */
    TransportClient getOrCreateClient(String host, int port)
                                      throws TransportException;

    /**
     * Close a client from the {@link ConnectionManager}
     * @param connectionId {@link ConnectionId}
     */
    void closeClient(ConnectionId connectionId);

    /**
     * Shutdown the client connection manager.
     */
    void shutdownClients();

    /**
     * Shutdown the client connection manager and server.
     */
    void shutdown();
}
