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

package org.apache.hugegraph.computer.core.network;

import java.net.InetSocketAddress;

import org.apache.hugegraph.computer.core.config.Config;

/**
 * This is used for worker that receives data.
 */
public interface TransportServer {

    /**
     * Startup server, return the port listened.
     */
    int listen(Config config, MessageHandler serverHandler);

    /**
     * Stop the server.
     */
    void shutdown();

    /**
     * To check whether the server is bound to use.
     * @return true if server is bound.
     */
    boolean bound();

    /**
     * Get the bind {@link InetSocketAddress}
     */
    InetSocketAddress bindAddress();

    /**
     * Get the bind IP
     */
    String ip();

    /**
     * Get the bind port
     */
    int port();
}
