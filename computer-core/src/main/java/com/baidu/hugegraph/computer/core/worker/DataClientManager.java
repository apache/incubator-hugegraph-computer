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

package com.baidu.hugegraph.computer.core.worker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.buffer.SortedBufferQueue;
import com.baidu.hugegraph.computer.core.buffer.SortedBufferQueuePool;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.ClientHandler;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.computer.core.network.connection.ConnectionManager;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.util.Log;

public class DataClientManager implements Manager {

    public static final Logger LOG = Log.logger(DataClientManager.class);

    public static final String NAME = "data_client";

    private final ConnectionManager connectionManager;
    // The thread used to send vertex/message, only one is enough
    private final SendExecutor sendExecutor;
    // Each target worker has a TransportClient
    private final Map<Integer, TransportClient> clients;
    private SortedBufferQueuePool queuePool;

    private final AtomicInteger emptyQueueCounter;
    private final AtomicInteger busyClientCounter;
    private final BarrierEvent notBusyEvent;

    public DataClientManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        this.sendExecutor = new SendExecutor();
        this.clients = new ConcurrentHashMap<>();
        this.queuePool = null;
        this.emptyQueueCounter = new AtomicInteger();
        this.busyClientCounter = new AtomicInteger();
        this.notBusyEvent = new BarrierEvent();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        ClientHandler availableHandler = new ClientHandler() {
            @Override
            public void sendAvailable(ConnectionId connectionId) {
                LOG.debug("Channel for connectionId {} is available",
                          connectionId);
                notBusyEvent.signalAll();
            }

            @Override
            public void channelActive(ConnectionId connectionId) {
            }

            @Override
            public void channelInactive(ConnectionId connectionId) {
            }

            @Override
            public void exceptionCaught(TransportException cause,
                                        ConnectionId connectionId) {
                LOG.error("Chananel for connectionId {} occurred exception",
                          connectionId, cause);
                connectionManager.closeClient(connectionId);
            }
        };
        this.connectionManager.initClientManager(config, availableHandler);
        this.sendExecutor.start();
    }

    @Override
    public void close(Config config) {
        this.sendExecutor.interrupt();
        this.connectionManager.shutdownClients();
    }

    public void connect(int workerId, String hostname, int dataPort) {
        try {
            TransportClient client = this.connectionManager.getOrCreateClient(
                                                            hostname, dataPort);
            this.clients.put(workerId, client);
        } catch (TransportException e) {
            throw new ComputerException("Failed to connect worker server {}:{}",
                                        hostname, dataPort);
        }
    }

    public void bufferQueuePool(SortedBufferQueuePool bufferQueuePool) {
        this.queuePool = bufferQueuePool;
    }

    private void waitAnyQueueAvailable() {
        this.queuePool.waitUntilAnyQueueNotEmpty();
    }

    private void waitAnyClientAvailable() {
        try {
            this.notBusyEvent.await();
        } catch (InterruptedException e) {
            throw new ComputerException("Waiting any client not busy " +
                                        "was interrupted");
        }
    }

    private class SendExecutor extends Thread {

        @Override
        public void run() {
            while (!this.isInterrupted()) {
                for (Map.Entry<Integer, TransportClient> entry :
                     clients.entrySet()) {
                    int workerId = entry.getKey();
                    TransportClient client = entry.getValue();
                    try {
                        this.doSend(workerId, client);
                    } catch (TransportException e) {
                        // TODO：How to deal with the exception here
                        e.printStackTrace();
                    }
                }
            }
            LOG.debug("Send Executor finish loop");
        }

        private void doSend(int workerId, TransportClient client)
                            throws TransportException {
            // Get the queue corresponding to the current client
            SortedBufferQueue queue = queuePool.get(workerId);
            if (queue.isEmpty()) {
                int count = emptyQueueCounter.incrementAndGet();
                if (count < clients.size()) {
                    return;
                }
                /*
                 * If all queues are empty (maybe the sending speed is faster
                 * than the upstream write speed), the sending thread should
                 * suspends itself
                 */
                waitAnyQueueAvailable();
                emptyQueueCounter.decrementAndGet();
            }

            SortedBufferEvent event = queue.peek();
            if (event == SortedBufferEvent.START) {
                client.startSession();
                LOG.info("Start session linked to {}", workerId);
                queue.poll();
                return;
            } else if (event == SortedBufferEvent.END) {
                client.finishSession();
                LOG.info("Finish session linked to {}", workerId);
                queue.poll();
                return;
            }

            if (client.send(event.messageType(), event.partitionId(),
                            event.sortedBuffer())) {
                // Pop up the element after sending successfully
                queue.poll();
            } else {
                int count = busyClientCounter.incrementAndGet();
                if (count == clients.size()) {
                    /*
                     * If all client are busy, let send thread wait
                     * until any client is available
                     */
                    waitAnyClientAvailable();
                }
            }
        }
    }
}
