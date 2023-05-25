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

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.computer.core.network.connection.ConnectionManager;
import org.apache.hugegraph.computer.core.sender.QueuedMessageSender;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class DataClientManager implements Manager {

    public static final Logger LOG = Log.logger(DataClientManager.class);

    public static final String NAME = "data_client";

    private final ConnectionManager connManager;
    private final QueuedMessageSender sender;

    public DataClientManager(ConnectionManager connManager,
                             ComputerContext context) {
        this.connManager = connManager;
        this.sender = new QueuedMessageSender(context.config());
    }

    public QueuedMessageSender sender() {
        return this.sender;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        ClientHandler clientHandler = new DataClientHandler(
                                      this.sender.notBusyNotifier());
        this.connManager.initClientManager(config, clientHandler);
        LOG.info("DataClientManager inited");
    }

    @Override
    public void inited(Config config) {
        this.sender.init();
    }

    @Override
    public void close(Config config) {
        try {
            this.sender.close();
        } finally {
            this.connManager.shutdownClients();
        }
        LOG.info("DataClientManager closed");
    }

    public void connect(int workerId, String hostname, int dataPort) {
        try {
            TransportClient client = this.connManager.getOrCreateClient(
                                     hostname, dataPort);
            LOG.info("Successfully connect to worker: {}({}:{})",
                     workerId, hostname, dataPort);
            this.sender.addWorkerClient(workerId, client);
        } catch (TransportException e) {
            throw new ComputerException(
                      "Failed to connect to worker: %s(%s:%s)",
                      workerId, hostname, dataPort);
        }
    }

    private class DataClientHandler implements ClientHandler {

        private final Runnable notBusyNotifier;

        public DataClientHandler(Runnable notBusyNotifier) {
            E.checkNotNull(notBusyNotifier,
                           "The not-busy notifier can't be null");
            this.notBusyNotifier = notBusyNotifier;
        }

        @Override
        public void sendAvailable(ConnectionId connectionId) {
            LOG.debug("Channel for connectionId {} is available", connectionId);
            this.notBusyNotifier.run();
        }

        @Override
        public void onChannelActive(ConnectionId connectionId) {
            LOG.debug("Channel for connectionId {} is active", connectionId);
        }

        @Override
        public void onChannelInactive(ConnectionId connectionId) {
            LOG.debug("Channel for connectionId {} is inactive", connectionId);
        }

        @Override
        public void exceptionCaught(TransportException cause,
                                    ConnectionId connectionId) {
            LOG.error("Channel for connectionId {} occurred exception",
                      connectionId, cause);
            DataClientManager.this.sender.transportExceptionCaught(cause, connectionId);
        }
    }
}
