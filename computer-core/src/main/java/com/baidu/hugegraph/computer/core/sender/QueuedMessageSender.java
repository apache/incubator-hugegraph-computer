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

    private static final String PREFIX = "send-executor";

    // All target workers share one message queue
    private final SortedBufferQueue queue;
    // Each target worker has a WorkerChannel
    private final WorkerChannel[] channels;
    // The thread used to send vertex/message, only one is enough
    private final Thread sendExecutor;
    private final BarrierEvent anyClientNotBusyEvent;
    // Just one thread modify these
    private int pendingMsgCount;
    private int busyClientCount;

    public QueuedMessageSender(Config config) {
        this.queue = new SortedBufferQueue();
        int workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
        // NOTE: the workerId start from 1
        this.channels = new WorkerChannel[workerCount];
        this.sendExecutor = new Thread(new Sender(), PREFIX);
        this.anyClientNotBusyEvent = new BarrierEvent();
        this.pendingMsgCount = 0;
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
        this.channels[workerId - 1] = channel;
        LOG.info("Add client {} for worker {}",
                 client.connectionId(), workerId);
    }

    @Override
    public CompletableFuture<Void> send(int workerId, MessageType type)
                                        throws InterruptedException {
        WorkerChannel channel = this.channels[workerId - 1];
        CompletableFuture<Void> future = channel.newFuture();
        future.whenComplete((r, e) -> {
            channel.resetFuture(future);
        });
        this.queue.put(new QueuedMessage(-1, workerId, type, null));
        return future;
    }

    @Override
    public void send(int workerId, QueuedMessage message)
                     throws InterruptedException {
        this.queue.put(message);
    }

    public Runnable notBusyNotifier() {
        // DataClientHandler.sendAvailable will call it
        return this.anyClientNotBusyEvent::signal;
    }

    private class Sender implements Runnable {

        @Override
        public void run() {
            LOG.info("Start run send exector");
            Thread thread = Thread.currentThread();
            while (!thread.isInterrupted()) {
                try {
                    // Handle all pending messages first
                    for (int i = 0; pendingMsgCount > 0 && i < channels.length;
                         i++) {
                        WorkerChannel channel = channels[i];
                        if (channel.pendingMessage != null) {
                            QueuedMessageSender.this.doSend(
                                    channel.pendingMessage);
                        }
                    }

                    // Take message from queue
                    QueuedMessage message = queue.take();
                    QueuedMessageSender.this.doSend(message);
                } catch (InterruptedException e) {
                    // Reset interrupted flag
                    Thread.currentThread().interrupt();
                    // Any client is active means that sending task in running
                    if (activeClientCount() > 0) {
                        throw new ComputerException(
                                  "Interrupted when waiting for message " +
                                  "queue not empty");
                    }
                } catch (TransportException e) {
                    // TODO: should handle this in main workflow thread
                    throw new ComputerException("Failed to send message", e);
                }
            }
            LOG.info("Finish run send exector");
        }
    }

    private void doSend(QueuedMessage message) throws TransportException,
                                                      InterruptedException {
        WorkerChannel channel = this.channels[message.workerId() - 1];
        switch (message.type()) {
            case START:
                this.sendStartMessage(channel);
                break;
            case FINISH:
                this.sendFinishMessage(channel);
                break;
            default:
                this.sendDataMessage(channel, message);
                break;
        }
    }

    private void sendStartMessage(WorkerChannel channel)
                                  throws TransportException {
        channel.client.startSessionAsync().whenComplete((r, e) -> {
            CompletableFuture<Void> future = channel.futureRef.get();
            assert future != null;

            if (e != null) {
                LOG.info("Failed to start session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                future.completeExceptionally(e);
            } else {
                LOG.info("Start session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
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
                LOG.info("Failed to finish session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                future.completeExceptionally(e);
            } else {
                LOG.info("Finish session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                future.complete(null);
            }
        });
    }

    private void sendDataMessage(WorkerChannel channel, QueuedMessage message)
                                 throws TransportException {
        if (channel.client.send(message.type(), message.partitionId(),
                                message.buffer())) {
            if (this.pendingMsgCount > 0) {
                --this.pendingMsgCount;
            }
            channel.pendingMessage = null;
        } else {
            ++this.pendingMsgCount;
            channel.pendingMessage = message;
            if (++this.busyClientCount >= this.channels.length) {
                /*
                 * If all clients are busy, let send thread wait
                 * until any client is available
                 */
                this.waitAnyClientNotBusy();
                --this.busyClientCount;
            }
        }
    }

    private void waitAnyClientNotBusy() {
        try {
            this.anyClientNotBusyEvent.await();
        } catch (InterruptedException e) {
            // Reset interrupted flag
            Thread.currentThread().interrupt();
            throw new ComputerException("Waiting any client not busy " +
                                        "was interrupted");
        } finally {
            this.anyClientNotBusyEvent.reset();
        }
    }

    private int activeClientCount() {
        int count = 0;
        for (WorkerChannel channel : this.channels) {
            if (channel.client.active()) {
                count++;
            }
        }
        return count;
    }

    private static class WorkerChannel {

        private final int workerId;
        // Each target worker has a TransportClient
        private final TransportClient client;
        private final AtomicReference<CompletableFuture<Void>> futureRef;
        private QueuedMessage pendingMessage;

        public WorkerChannel(int workerId, TransportClient client) {
            this.workerId = workerId;
            this.client = client;
            this.futureRef = new AtomicReference<>();
            this.pendingMessage = null;
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
    }
}
