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

package com.baidu.hugegraph.computer.core.network;

import java.net.InetSocketAddress;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.connection.ConnectionManager;
import com.baidu.hugegraph.computer.core.network.connection.TransportConnectionManager;

public class DataServerManager implements Manager {

    public static final String NAME = "data_server";

    private final ConnectionManager connectionManager;
    private final MessageHandler messageHandler;

    public DataServerManager(MessageHandler messageHandler) {
        this.connectionManager = new TransportConnectionManager();
        this.messageHandler = messageHandler;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        this.connectionManager.startServer(config, this.messageHandler);
    }

    @Override
    public void close(Config config) {
        this.connectionManager.shutdownServer();
    }

    public InetSocketAddress address() {
        return this.connectionManager.getServer().bindAddress();
    }
}
