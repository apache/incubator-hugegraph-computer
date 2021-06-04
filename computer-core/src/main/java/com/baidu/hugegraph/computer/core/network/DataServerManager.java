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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.connection.ConnectionManager;
import com.baidu.hugegraph.computer.core.network.connection.TransportConnectionManager;
import com.baidu.hugegraph.util.Log;

public class DataServerManager implements Manager {

    private static final Logger LOG = Log.logger(DataServerManager.class);
    public static final String NAME = "data_server";

    private final ConnectionManager connectionManager;
    private final MessageHandler messageHandler;

    public DataServerManager(MessageHandler messageHandler) {
        // TODO: move connectionManager to WorkerService.
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
        LOG.info("DataServerManager initialized with address '{}'",
                 this.address());
    }

    @Override
    public void close(Config config) {
        InetSocketAddress address = this.address();
        this.connectionManager.shutdownServer();
        LOG.info("DataServerManager closed with address '{}'", address);
    }

    public InetSocketAddress address() {
        return this.connectionManager.getServer().bindAddress();
    }
}
