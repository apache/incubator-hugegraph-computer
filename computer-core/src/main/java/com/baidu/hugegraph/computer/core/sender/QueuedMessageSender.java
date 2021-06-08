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
    private final WorkerChannel[] workerChannels;
    // The thread used to send vertex/message, only one is enough
    private final Thread sendExecutor;
    // Hungry means that queue is empty
    private final AtomicReference<Boolean> hungry;
    private final BarrierEvent anyQueueNotEmptyEvent;
    private final BarrierEvent anyClientNotBusyEvent;
    // Just one thread modify these
    private int activeClientCount;
    private int emptyQueueCount;
    private int busyClientCount;

    public QueuedMessageSender(Config config) {
        int workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
        // NOTE: the workerId start from 1
        this.workerChannels = new WorkerChannel[workerCount];
        this.sendExecutor = new Thread(new Sender(), PREFIX);
        this.hungry = new AtomicReference<>(true);
        this.anyQueueNotEmptyEvent = new BarrierEvent();
        this.anyClientNotBusyEvent = new BarrierEvent();
        this.queue = new SortedBufferQueue(this.hungry,
                                           this.anyQueueNotEmptyEvent::signal);
        this.activeClientCount = 0;
        this.emptyQueueCount = 0;
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
            throw new ComputerException("Failed to join send executor thread",
                                        e);
        }
    }

    public void addWorkerClient(int workerId, TransportClient client) {
        WorkerChannel channel = new WorkerChannel(workerId, client);
        this.workerChannels[workerId - 1] = channel;
        LOG.info("Add client {} for worker {}",
                 client.connectionId(), workerId);
    }

    @Override
    public CompletableFuture<Void> send(int workerId,
                                        QueuedMessage message)
                                        throws InterruptedException {
        WorkerChannel channel = this.workerChannels[workerId - 1];
        if (channel == null)  {
            throw new ComputerException("Invalid workerId %s", workerId);
        }
        if (message.type() == MessageType.START ||
            message.type() == MessageType.FINISH) {
            channel.newFuture();
        }
        this.queue.put(message);
        return channel.futureRef.get();
    }

    @Override
    public void reset(int workerId, CompletableFuture<Void> future) {
        WorkerChannel channel = this.workerChannels[workerId - 1];
        if (!channel.futureRef.compareAndSet(future, null)) {
            throw new ComputerException("Failed to reset futureRef");
        }
    }

    public Runnable notBusyNotifier() {
        // DataClientHandler.sendAvailable will call it
        return this.anyClientNotBusyEvent::signal;
    }

    private int workerCount() {
        return this.workerChannels.length;
    }

    private class Sender implements Runnable {

        @Override
        public void run() {
            LOG.info("Start run send exector");
            Thread thread = Thread.currentThread();
            while (!thread.isInterrupted()) {
                try {
                    QueuedMessageSender.this.doSend();
                } catch (Exception e) {
                    // TODO: should handle this in main workflow thread
                    throw new ComputerException("Failed to send message", e);
                }
            }
            LOG.info("Finish run send exector");
        }
    }

    private void doSend() throws TransportException, InterruptedException {
        QueuedMessage message = this.queue.peek();
        if (message == null) {
            this.handleAllQueueEmpty();
            return;
        }
        WorkerChannel channel = this.workerChannels[message.workerId() - 1];
        switch (message.type()) {
            case START:
                this.handleStartMessage(channel);
                break;
            case FINISH:
                this.handleFinishMessage(channel);
                break;
            default:
                this.handleDataMessage(channel, message);
                break;
        }
    }

    private void handleAllQueueEmpty() {
        if (++this.emptyQueueCount >= this.workerCount()) {
            /*
             * If all queues are empty (maybe the sending speed is faster
             * than the upstream write speed), the sending thread should
             * suspends itself
             */
            this.hungry.set(true);
            this.waitAnyQueueNotEmpty();
            this.hungry.set(false);
            --this.emptyQueueCount;
        }
    }

    private void handleStartMessage(WorkerChannel channel) throws
                                    TransportException, InterruptedException {
        channel.client.startSessionAsync().whenComplete((r, e) -> {
            CompletableFuture<Void> future = channel.futureRef.get();
            if (future == null) {
                throw new ComputerException("The future of worker %s for " +
                                            "message %s must be setted before",
                                            channel.workerId,
                                            MessageType.START);
            }

            if (e != null) {
                LOG.info("Failed to start session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                future.completeExceptionally(e);
            } else {
                LOG.info("Start session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                ++this.activeClientCount;
                future.complete(null);
            }
        });
        this.queue.take();
    }

    private void handleFinishMessage(WorkerChannel channel) throws
                                     TransportException, InterruptedException {
        channel.client.finishSessionAsync().whenComplete((r, e) -> {
            CompletableFuture<Void> future = channel.futureRef.get();
            if (future == null) {
                throw new ComputerException("The future of worker %s for " +
                                            "message %s must be setted before",
                                            channel.workerId,
                                            MessageType.FINISH);
            }

            if (e != null) {
                LOG.info("Failed to finish session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                future.completeExceptionally(e);
            } else {
                LOG.info("Finish session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                --this.activeClientCount;
                future.complete(null);
            }
        });
        this.queue.take();
    }

    private void handleDataMessage(WorkerChannel channel,
                                   QueuedMessage message) throws
                                   TransportException, InterruptedException {
        if (channel.client.send(message.type(), message.partitionId(),
                                message.buffer())) {
            // Pop up the element after sending successfully
            this.queue.take();
        } else {
            if (++this.busyClientCount == this.workerCount()) {
                /*
                 * If all clients are busy, let send thread wait
                 * until any client is available
                 */
                this.waitAnyClientNotBusy();
                --this.busyClientCount;
            }
        }
    }

    private void waitAnyQueueNotEmpty() {
        try {
            this.anyQueueNotEmptyEvent.await();
        } catch (InterruptedException e) {
            // Reset interrupted flag
            Thread.currentThread().interrupt();
            // Any client is active means that the sending task in running state
            if (this.activeClientCount > 0) {
                throw new ComputerException("Waiting any buffer queue not " +
                                            "empty was interrupted");
            }
        } finally {
            this.anyQueueNotEmptyEvent.reset();
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

        public void newFuture() {
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (!this.futureRef.compareAndSet(null, future)) {
                throw new ComputerException("The origin future must be null");
            }
        }
    }
}
