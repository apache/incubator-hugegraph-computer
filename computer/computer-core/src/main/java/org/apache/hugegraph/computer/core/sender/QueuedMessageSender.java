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

package org.apache.hugegraph.computer.core.sender;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.TransportClient;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.concurrent.BarrierEvent;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class QueuedMessageSender implements MessageSender {

    public static final Logger LOG = Log.logger(QueuedMessageSender.class);

    private static final String NAME = "send-executor";

    // Each target worker has a WorkerChannel
    private final WorkerChannel[] channels;
    // The thread used to send vertex/message, only one is enough
    private final Thread sendExecutor;
    private final BarrierEvent anyQueueNotEmptyEvent;
    private final BarrierEvent anyClientNotBusyEvent;

    public QueuedMessageSender(Config config) {
        int workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
        // NOTE: the workerId start from 1
        this.channels = new WorkerChannel[workerCount];
        // TODO: pass send-executor in and share executor with others
        this.sendExecutor = new Thread(new Sender(), NAME);
        this.anyQueueNotEmptyEvent = new BarrierEvent();
        this.anyClientNotBusyEvent = new BarrierEvent();
    }

    public void init() {
        for (WorkerChannel channel : this.channels) {
            E.checkNotNull(channel, "channel");
        }
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
        MessageQueue queue = new MessageQueue(
                             this.anyQueueNotEmptyEvent::signal);
        WorkerChannel channel = new WorkerChannel(workerId, queue, client);
        this.channels[channelId(workerId)] = channel;
        LOG.info("Add client {} for worker {}",
                 client.connectionId(), workerId);
    }

    @Override
    public CompletableFuture<Void> send(int workerId, MessageType type)
                                        throws InterruptedException {
        WorkerChannel channel = this.channels[channelId(workerId)];
        CompletableFuture<Void> future = channel.newFuture();
        future.whenComplete((r, e) -> {
            channel.resetFuture(future);
        });
        /*
         * Control message just need message type is enough,
         * partitionId = -1 and buffer = null represents a meaningless value
         */
        channel.queue.put(new QueuedMessage(-1, type, null));
        return future;
    }

    @Override
    public void send(int workerId, QueuedMessage message)
                     throws InterruptedException {
        WorkerChannel channel = this.channels[channelId(workerId)];
        channel.queue.put(message);
    }

    @Override
    public void transportExceptionCaught(TransportException cause, ConnectionId connectionId) {
        for (WorkerChannel channel : this.channels) {
            if (channel.client.connectionId().equals(connectionId)) {
                channel.futureRef.get().completeExceptionally(cause);
            }
        }
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
                    int emptyQueueCount = 0;
                    int busyClientCount = 0;
                    for (WorkerChannel channel : channels) {
                        QueuedMessage message = channel.queue.peek();
                        if (message == null) {
                            ++emptyQueueCount;
                            continue;
                        }
                        if (channel.doSend(message)) {
                            // Only consume the message after it is sent
                            channel.queue.take();
                        } else {
                            ++busyClientCount;
                        }
                    }
                    int channelCount = channels.length;
                    /*
                     * If all queues are empty, let send thread wait
                     * until any queue is available
                     */
                    if (emptyQueueCount >= channelCount) {
                        LOG.debug("The send executor was blocked " +
                                  "to wait any queue not empty");
                        QueuedMessageSender.this.waitAnyQueueNotEmpty();
                    }
                    /*
                     * If all clients are busy, let send thread wait
                     * until any client is available
                     */
                    if (busyClientCount >= channelCount) {
                        LOG.debug("The send executor was blocked " +
                                  "to wait any client not busy");
                        QueuedMessageSender.this.waitAnyClientNotBusy();
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

    private void waitAnyQueueNotEmpty() {
        try {
            this.anyQueueNotEmptyEvent.await();
        } catch (InterruptedException e) {
            // Reset interrupted flag
            Thread.currentThread().interrupt();
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
            throw new ComputerException("Interrupted when waiting any client " +
                                        "not busy");
        } finally {
            this.anyClientNotBusyEvent.reset();
        }
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

    private static int channelId(int workerId) {
        assert workerId > 0;
        return workerId - 1;
    }

    private static class WorkerChannel {

        private final int workerId;
        // Each target worker has a MessageQueue
        private final MessageQueue queue;
        // Each target worker has a TransportClient
        private final TransportClient client;
        private final AtomicReference<CompletableFuture<Void>> futureRef;

        public WorkerChannel(int workerId, MessageQueue queue,
                             TransportClient client) {
            this.workerId = workerId;
            this.queue = queue;
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

        public boolean doSend(QueuedMessage message)
                              throws TransportException, InterruptedException {
            switch (message.type()) {
                case START:
                    this.sendStartMessage();
                    return true;
                case FINISH:
                    this.sendFinishMessage();
                    return true;
                default:
                    return this.sendDataMessage(message);
            }
        }

        public void sendStartMessage() throws TransportException {
            this.client.startSessionAsync().whenComplete((r, e) -> {
                CompletableFuture<Void> future = this.futureRef.get();
                assert future != null;

                if (e != null) {
                    LOG.info("Failed to start session connected to {}", this);
                    future.completeExceptionally(e);
                } else {
                    LOG.info("Start session connected to {}", this);
                    future.complete(null);
                }
            });
        }

        public void sendFinishMessage() throws TransportException {
            this.client.finishSessionAsync().whenComplete((r, e) -> {
                CompletableFuture<Void> future = this.futureRef.get();
                assert future != null;

                if (e != null) {
                    LOG.info("Failed to finish session connected to {}", this);
                    future.completeExceptionally(e);
                } else {
                    LOG.info("Finish session connected to {}", this);
                    future.complete(null);
                }
            });
        }

        public boolean sendDataMessage(QueuedMessage message)
                                       throws TransportException {
            return this.client.send(message.type(), message.partitionId(),
                                    message.buffer());
        }

        @Override
        public String toString() {
            return String.format("workerId=%s(remoteAddress=%s)",
                                 this.workerId, this.client.remoteAddress());
        }
    }
}
