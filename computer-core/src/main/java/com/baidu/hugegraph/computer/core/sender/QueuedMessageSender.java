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

package com.baidu.hugegraph.computer.core.sender;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.util.Log;

public class QueuedMessageSender implements MessageSender {

    public static final Logger LOG = Log.logger(QueuedMessageSender.class);

    private static final String NAME = "send-executor";

    // All target worker share one message queue
    private final MultiQueue queue;
    // Each target worker has a WorkerChannel
    private final WorkerChannel[] channels;
    // The thread used to send vertex/message, only one is enough
    private final Thread sendExecutor;
    private final BarrierEvent anyClientNotBusyEvent;
    // Just one thread modify it
    private int busyClientCount;

    public QueuedMessageSender(Config config) {
        int workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
        this.queue = new MultiQueue(workerCount);
        // NOTE: the workerId start from 1
        this.channels = new WorkerChannel[workerCount];
        // TODO: pass send-executor in and share executor with others
        this.sendExecutor = new Thread(new Sender(), NAME);
        this.anyClientNotBusyEvent = new BarrierEvent();
        this.busyClientCount = 0;
    }

    public void init() {
        this.sendExecutor.start();
    }

    public void close() {
        this.sendExecutor.interrupt();
        try {
            this.sendExecutor.join();
        } catch (InterruptedException e) {
            throw new ComputerException("Interrupted when waiting for " +
                                        "send-executor to stop", e);
        }
    }

    public void addWorkerClient(int workerId, TransportClient client) {
        WorkerChannel channel = new WorkerChannel(workerId, client);
        this.channels[this.normalize(workerId)] = channel;
        LOG.info("Add client {} for worker {}",
                 client.connectionId(), workerId);
    }

    @Override
    public CompletableFuture<Void> send(int workerId, MessageType type)
                                        throws InterruptedException {
        WorkerChannel channel = this.channels[this.normalize(workerId)];
        CompletableFuture<Void> future = channel.newFuture();
        future.whenComplete((r, e) -> {
            channel.resetFuture(future);
        });
        this.queue.put(this.normalize(workerId),
                       new QueuedMessage(-1, workerId, type, null));
        return future;
    }

    @Override
    public void send(int workerId, QueuedMessage message)
                     throws InterruptedException {
        this.queue.put(this.normalize(workerId), message);
    }

    public Runnable notBusyNotifier() {
        /*
         * DataClientHandler.sendAvailable() will call it when client
         * is available
         */
        return this.anyClientNotBusyEvent::signal;
    }

    private class Sender implements Runnable {

        @Override
        public void run() {
            LOG.info("The send-executor is running");
            Thread thread = Thread.currentThread();
            while (!thread.isInterrupted()) {
                try {
                    QueuedMessage message = queue.take();
                    if (!QueuedMessageSender.this.doSend(message)) {
                        int workerId = message.workerId();
                        queue.putAtFront(normalize(workerId), message);
                    }
                } catch (InterruptedException e) {
                    // Reset interrupted flag
                    thread.interrupt();
                    // Any client is active means that sending task in running
                    if (QueuedMessageSender.this.activeClientCount() > 0) {
                        throw new ComputerException(
                                  "Interrupted when waiting for message " +
                                  "queue not empty");
                    }
                } catch (TransportException e) {
                    // TODO: should handle this in main workflow thread
                    throw new ComputerException("Failed to send message", e);
                }
            }
            LOG.info("The send-executor is terminated");
        }
    }

    private boolean doSend(QueuedMessage message) throws TransportException,
                                                         InterruptedException {
        WorkerChannel channel = this.channels[message.workerId() - 1];
        switch (message.type()) {
            case START:
                this.sendStartMessage(channel);
                return true;
            case FINISH:
                this.sendFinishMessage(channel);
                return true;
            default:
                return this.sendDataMessage(channel, message);
        }
    }

    private void sendStartMessage(WorkerChannel channel)
                                  throws TransportException {
        channel.client.startSessionAsync().whenComplete((r, e) -> {
            CompletableFuture<Void> future = channel.futureRef.get();
            assert future != null;

            if (e != null) {
                LOG.info("Failed to start session connected to {}", channel);
                future.completeExceptionally(e);
            } else {
                LOG.info("Start session connected to {}", channel);
                future.complete(null);
            }
        });
    }

    private void sendFinishMessage(WorkerChannel channel)
                                   throws TransportException {
        channel.client.finishSessionAsync().whenComplete((r, e) -> {
            CompletableFuture<Void> future = channel.futureRef.get();
            assert future != null;

            if (e != null) {
                LOG.info("Failed to finish session connected to {}", channel);
                future.completeExceptionally(e);
            } else {
                LOG.info("Finish session connected to {}", channel);
                future.complete(null);
            }
        });
    }

    private boolean sendDataMessage(WorkerChannel channel,
                                    QueuedMessage message)
                                    throws TransportException {
        if (channel.client.send(message.type(), message.partitionId(),
                                message.buffer())) {
            return true;
        }
        if (++this.busyClientCount >= this.channels.length) {
            /*
             * If all clients are busy, let send thread wait
             * until any client is available
             */
            this.waitAnyClientNotBusy();
            --this.busyClientCount;
        }
        return false;
    }

    private void waitAnyClientNotBusy() {
        try {
            this.anyClientNotBusyEvent.await();
        } catch (InterruptedException e) {
            // Reset interrupted flag
            Thread.currentThread().interrupt();
            throw new ComputerException("Interrupted when waiting any client " +
                                        "not busy");
        } finally {
            this.anyClientNotBusyEvent.reset();
        }
    }

    private int normalize(int workerId) {
        return workerId - 1;
    }

    private int activeClientCount() {
        int count = 0;
        for (WorkerChannel channel : this.channels) {
            if (channel.client.sessionActive()) {
                ++count;
            }
        }
        return count;
    }

    private static class WorkerChannel {

        private final int workerId;
        // Each target worker has a TransportClient
        private final TransportClient client;
        private final AtomicReference<CompletableFuture<Void>> futureRef;

        public WorkerChannel(int workerId, TransportClient client) {
            this.workerId = workerId;
            this.client = client;
            this.futureRef = new AtomicReference<>();
        }

        public CompletableFuture<Void> newFuture() {
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (!this.futureRef.compareAndSet(null, future)) {
                throw new ComputerException("The origin future must be null");
            }
            return future;
        }

        public void resetFuture(CompletableFuture<Void> future) {
            if (!this.futureRef.compareAndSet(future, null)) {
                throw new ComputerException("Failed to reset futureRef, " +
                                            "expect future object is %s, " +
                                            "but some thread modified it",
                                            future);
            }
        }

        @Override
        public String toString() {
            return String.format("workerId=%s(remoteAddress=%s)",
                                 this.workerId, this.client.remoteAddress());
        }
    }
}
