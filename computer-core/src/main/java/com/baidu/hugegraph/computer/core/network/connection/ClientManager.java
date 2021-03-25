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

import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.Transport4Client;

public interface ClientManager {

    /**
     * Startup the clientManager
     */
    void startup();


    /**
     * Get a {@link Transport4Client} instance from the connection pool first.
     * If {it is not found or not active, create a new one.
     * @param connectionID {@link ConnectionID}
     */
    Transport4Client getOrCreateTransport4Client(ConnectionID connectionID)
                                                  throws IOException;


    /**
     * Get a {@link Transport4Client} instance from the connection pool first.
     * If {it is not found or not active, create a new one.
     * @param host the hostName or Ip
     * @param port the port
     */
    Transport4Client getOrCreateTransport4Client(String host, int port)
                                                  throws IOException;

    /**
     * remove a client from the connection pool
     * @param connectionID {@link ConnectionID}
     */
    void removeClient(ConnectionID connectionID);


    /**
     * shutdown the clientManager
     */
    void shutdown() throws IOException;
}
