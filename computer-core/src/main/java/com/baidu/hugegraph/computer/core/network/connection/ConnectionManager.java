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

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.ClientHandler;
import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.computer.core.network.TransportServer;

/**
 * This is used for unified manage server and client connection.
 */
public interface ConnectionManager {

    /**
     * Initialize the client connection manager.
     * This method is called only once.
     */
    void initClientManager(Config config, ClientHandler clientHandler);

    /**
     * Get a {@link TransportClient} instance from the connection pool first.
     * If {it is not found or not active, create a new one.
     * @param connectionID {@link ConnectionID}
     */
    TransportClient getOrCreateClient(ConnectionID connectionID)
                                      throws IOException;

    /**
     * Get a {@link TransportClient} instance from the connection pool first.
     * If {it is not found or not active, create a new one.
     * @param host the hostName or Ip
     * @param port the port
     */
    TransportClient getOrCreateClient(String host, int port) throws IOException;

    /**
     * Close a client from the {@link ConnectionManager}
     * @param connectionID {@link ConnectionID}
     */
    void closeClient(ConnectionID connectionID);

    /**
     * Shutdown the client connection manager.
     */
    void shutdownClientManager();

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
     * Shutdown the ClientFactory and server.
     */
    void shutdown();
}
