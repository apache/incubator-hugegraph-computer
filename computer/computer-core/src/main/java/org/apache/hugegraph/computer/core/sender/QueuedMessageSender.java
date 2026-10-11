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
    private final AtomicReference<Throwable> fatalError;
    private volatile boolean closed;

    public QueuedMessageSender(Config config) {
        int workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
        // NOTE: the workerId start from 1
        this.channels = new WorkerChannel[workerCount];
        // TODO: pass send-executor in and share executor with others
        this.sendExecutor = new Thread(new Sender(), NAME);
        this.anyQueueNotEmptyEvent = new BarrierEvent();
        this.anyClientNotBusyEvent = new BarrierEvent();
        this.fatalError = new AtomicReference<>();
    }

    public void init() {
        for (WorkerChannel channel : this.channels) {
            E.checkNotNull(channel, "channel");
        }
        this.sendExecutor.start();
    }

    public void close() {
        this.closed = true;
        Throwable error = this.fatalError.get();
        this.failChannels(error != null ? error :
                          new ComputerException("Send-executor closed"));
        this.sendExecutor.interrupt();
        try {
            this.sendExecutor.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ComputerException("Interrupted when waiting for " +
                                        "send-executor to stop", e);
        }
    }

    @Override
    public void checkFatal() {
        Throwable error = this.fatalError.get();
        if (error != null) {
            throw new ComputerException("Send-executor encountered fatal error",
                                        error);
        }
    }

    private void recordFatal(Throwable error) {
        if (this.fatalError.compareAndSet(null, error)) {
            this.failChannels(error);
        }
    }

    private void failChannels(Throwable error) {
        for (WorkerChannel channel : this.channels) {
            if (channel != null) {
                channel.failDataSend(error);
            }
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
        this.checkFatal();
        E.checkArgument(type == MessageType.START ||
                        type == MessageType.FINISH,
                        "The control message type must be START or FINISH, " +
                        "but got '%s'", type);
        WorkerChannel channel = this.channels[channelId(workerId)];
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (!channel.setControlFuture(future)) {
            return future;
        }
        /*
         * Control message just need message type is enough,
         * partitionId = -1 and buffer = null represents a meaningless value
         */
        try {
            channel.queue.put(new QueuedMessage(-1, type, null, future));
        } catch (InterruptedException e) {
            channel.completeControlFuture(future, e);
            throw e;
        }
        return future;
    }

    @Override
    public void send(int workerId, QueuedMessage message)
                     throws InterruptedException {
        this.checkFatal();
        E.checkArgument(message.type() != null &&
                        message.type().category() == MessageType.Category.DATA,
                        "The queued message type must be DATA, but got '%s'",
                        message.type());
        WorkerChannel channel = this.channels[channelId(workerId)];
        channel.queue.put(message);
    }

    @Override
    public void transportExceptionCaught(TransportException cause, ConnectionId connectionId) {
        for (WorkerChannel channel : this.channels) {
            if (channel.client.connectionId().equals(connectionId)) {
                channel.transportExceptionCaught(cause);
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
            try {
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
                            try {
                                if (channel.doSend(message)) {
                                    // Only consume the message after it is sent
                                    channel.queue.take();
                                } else {
                                    ++busyClientCount;
                                }
                            } catch (TransportException | RuntimeException e) {
                                channel.failDataSend(e);
                                // Discard the failed data message to keep sending
                                channel.queue.take();
                                LOG.warn("Failed to send {} message to {}, " +
                                         "discard it", message.type(), channel, e);
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
                        if (QueuedMessageSender.this.closed) {
                            // Normal shutdown path
                            return;
                        }
                        // Any client is active means that sending task in running
                        if (QueuedMessageSender.this.activeClientCount() > 0) {
                            throw new ComputerException(
                                      "Interrupted when waiting for message " +
                                      "queue not empty");
                        }
                    }
                }
            } catch (Throwable t) {
                if (!QueuedMessageSender.this.closed) {
                    QueuedMessageSender.this.recordFatal(t);
                    LOG.error("The send-executor terminated unexpectedly", t);
                }
                return;
            } finally {
                LOG.info("The send-executor is terminated");
            }
        }
    }

    private void waitAnyQueueNotEmpty() {
        try {
            this.anyQueueNotEmptyEvent.await();
        } catch (InterruptedException e) {
            // Reset interrupted flag
            Thread.currentThread().interrupt();
            if (this.closed) {
                // Normal shutdown, do not treat as error
                return;
            }
            ComputerException error = new ComputerException(
                    "Interrupted when waiting any queue not empty");
            this.recordFatal(error);
            throw error;
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
            if (this.closed) {
                // Normal shutdown, do not treat as error
                return;
            }
            ComputerException error = new ComputerException(
                    "Interrupted when waiting any client not busy");
            this.recordFatal(error);
            throw error;
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
        private final AtomicReference<CompletableFuture<Void>> controlFutureRef;
        private final AtomicReference<Throwable> dataFailureRef;

        public WorkerChannel(int workerId, MessageQueue queue,
                             TransportClient client) {
            this.workerId = workerId;
            this.queue = queue;
            this.client = client;
            this.controlFutureRef = new AtomicReference<>();
            this.dataFailureRef = new AtomicReference<>();
        }

        public boolean doSend(QueuedMessage message)
                              throws TransportException, InterruptedException {
            switch (message.type()) {
                case START:
                    this.sendStartMessage(this.controlFuture(message));
                    return true;
                case FINISH:
                    this.sendFinishMessage(this.controlFuture(message));
                    return true;
                default:
                    return this.sendDataMessage(message);
            }
        }

        private CompletableFuture<Void> controlFuture(QueuedMessage message) {
            CompletableFuture<Void> future = message.controlFuture();
            E.checkState(future != null,
                         "The control future can't be null for message '%s'",
                         message.type());
            return future;
        }

        public void sendStartMessage(CompletableFuture<Void> future) {
            if (!this.controlFutureInFlight(future)) {
                return;
            }
            try {
                this.client.startSessionAsync().whenComplete((r, e) -> {
                    if (e != null) {
                        LOG.info("Failed to start session connected to {}", this);
                    } else {
                        LOG.info("Start session connected to {}", this);
                    }
                    this.completeControlFuture(future, e);
                });
            } catch (TransportException e) {
                this.completeControlFuture(future, e);
            } catch (RuntimeException e) {
                this.completeControlFuture(future, e);
            }
        }

        public void sendFinishMessage(CompletableFuture<Void> future) {
            if (!this.controlFutureInFlight(future)) {
                return;
            }
            try {
                this.client.finishSessionAsync().whenComplete((r, e) -> {
                    if (e != null) {
                        LOG.info("Failed to finish session connected to {}", this);
                    } else {
                        LOG.info("Finish session connected to {}", this);
                    }
                    this.completeControlFuture(future, e);
                });
            } catch (TransportException e) {
                this.completeControlFuture(future, e);
            } catch (RuntimeException e) {
                this.completeControlFuture(future, e);
            }
        }

        public void transportExceptionCaught(TransportException cause) {
            CompletableFuture<Void> future = this.controlFutureRef.get();
            if (future == null) {
                this.failDataSend(cause);
            } else {
                this.completeControlFuture(future, cause);
            }
        }

        public void failControlFuture(Throwable cause) {
            CompletableFuture<Void> future = this.controlFutureRef.getAndSet(null);
            if (future != null) {
                future.completeExceptionally(cause);
            }
        }

        public void failDataSend(Throwable cause) {
            this.dataFailureRef.compareAndSet(null, cause);
            this.failControlFuture(this.dataFailureRef.get());
        }

        private boolean setControlFuture(CompletableFuture<Void> future) {
            Throwable failure = this.dataFailureRef.get();
            if (failure != null) {
                future.completeExceptionally(failure);
                return false;
            }
            if (this.controlFutureRef.compareAndSet(null, future)) {
                failure = this.dataFailureRef.get();
                if (failure == null) {
                    return true;
                }
                this.completeControlFuture(future, failure);
                return false;
            }
            ComputerException e = new ComputerException(
                                  "The origin future must be null");
            future.completeExceptionally(e);
            return false;
        }

        private boolean controlFutureInFlight(CompletableFuture<Void> future) {
            return future != null && this.controlFutureRef.get() == future;
        }

        private void completeControlFuture(CompletableFuture<Void> future,
                                           Throwable cause) {
            E.checkState(future != null, "The control future can't be null");
            if (!this.controlFutureRef.compareAndSet(future, null)) {
                if (cause != null) {
                    this.failDataSend(cause);
                }
                return;
            }
            if (cause == null) {
                future.complete(null);
            } else {
                future.completeExceptionally(cause);
            }
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
