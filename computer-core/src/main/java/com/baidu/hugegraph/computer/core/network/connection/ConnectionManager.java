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
import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.Transport4Client;
import com.baidu.hugegraph.computer.core.network.Transport4Server;

public interface ConnectionManager {

    /**
     * Init the client connection manager.
     * This method is called only once.
     */
    void initClient(Config config);

    /**
     * Get a {@link Transport4Client} instance from the connection pool first.
     * If {it is not found or not active, create a new one.
     * @param connectionID {@link ConnectionID}
     */
    Transport4Client getOrCreateClient(ConnectionID connectionID)
                                       throws IOException;

    /**
     * Get a {@link Transport4Client} instance from the connection pool first.
     * If {it is not found or not active, create a new one.
     * @param host the hostName or Ip
     * @param port the port
     */
    Transport4Client getOrCreateClient(String host, int port)
                                       throws IOException;
    /**
     * Close a client from the {@link ConnectionManager}
     * @param client {@link Transport4Client}
     */
    void closeClient(Transport4Client client);

    /**
     * Shutdown the client connection manager.
     */
    void shutdownClient();

    /**
     * Start the server, return the port listened.
     * This method is called only once.
     */
    int startServer(Config config, MessageHandler handler);

    /**
     * Return the only one listened server.
     */
    Transport4Server getServer();

    /**
     * Shutdown the server.
     */
    void shutdownServer();

    /**
     * Shutdown the ClientFactory and server.
     */
    void shutdown();
}
