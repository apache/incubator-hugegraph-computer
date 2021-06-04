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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.util.Log;

public class QueuedMessageSender implements MessageSender {

    public static final Logger LOG = Log.logger(QueuedMessageSender.class);

    private static final String PREFIX = "send-executor-";

    // Each target worker has a WorkerChannel
    private final Map<Integer, WorkerChannel> workerChannels;
    // The thread used to send vertex/message, only one is enough
    private final Thread sendExecutor;
    private final BarrierEvent anyQueueNotEmptyEvent;
    private final BarrierEvent anyClientNotBusyEvent;
    // Just one thread modify these
    private int activeClientCount;
    private int emptyQueueCount;
    private int busyClientCount;

    public QueuedMessageSender() {
        this.workerChannels = new ConcurrentHashMap<>();
        this.sendExecutor = new Thread(new Sender(), PREFIX);
        this.anyQueueNotEmptyEvent = new BarrierEvent();
        this.anyClientNotBusyEvent = new BarrierEvent();
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
        SortedBufferQueue queue = new SortedBufferQueue(
                                  this.anyQueueNotEmptyEvent::signal);
        WorkerChannel channel = new WorkerChannel(workerId, queue, client);
        this.workerChannels.put(workerId, channel);
        LOG.info("Add channel for worker {}", workerId);
    }

    @Override
    public CompletableFuture<Void> send(int workerId,
                                        SortedBufferMessage message)
                                        throws InterruptedException {
        WorkerChannel channel = this.workerChannels.get(workerId);
        if (channel == null)  {
            throw new ComputerException("Invalid workerId %s", workerId);
        }
        channel.queue.put(message);
        channel.newFuture();
        return channel.future;
    }

    public Runnable notBusyNotifier() {
        // DataClientHandler.sendAvailable will call it
        return this.anyClientNotBusyEvent::signal;
    }

    private int workerCount() {
        return this.workerChannels.size();
    }

    private class Sender implements Runnable {

        @Override
        public void run() {
            LOG.debug("Start run send exector");
            Thread thread = Thread.currentThread();
            while (!thread.isInterrupted()) {
                try {
                    for (WorkerChannel channel : workerChannels.values()) {
                        QueuedMessageSender.this.doSend(channel);
                    }
                } catch (Exception e) {
                    // TODO: should handle this in main workflow thread
                    throw new ComputerException("Failed to send message", e);
                }
            }
            LOG.debug("Finish run send exector");
        }
    }

    private void doSend(WorkerChannel channel) throws TransportException,
                                                      InterruptedException {
        SortedBufferQueue queue = channel.queue;
        SortedBufferMessage message = queue.peek();
        if (message == null) {
            this.handleAllQueueEmpty();
            return;
        }
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
            this.waitAnyQueueNotEmpty();
            --this.emptyQueueCount;
        }
    }

    private void handleStartMessage(WorkerChannel channel) throws
                                    TransportException, InterruptedException {
        CompletableFuture<Void> future = channel.client.startSessionAsync();
        channel.queue.take();
        ++this.activeClientCount;

        future.whenComplete((r, e) -> {
            if (e != null) {
                LOG.info("Failed to start session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                channel.future.completeExceptionally(e);
            } else {
                LOG.info("Start session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                channel.future.complete(null);
            }
        });
    }

    private void handleFinishMessage(WorkerChannel channel) throws
                                     TransportException, InterruptedException {
        CompletableFuture<Void> future = channel.client.finishSessionAsync();
        channel.queue.take();
        --this.activeClientCount;

        future.whenComplete((r, e) -> {
            if (e != null) {
                LOG.info("Failed to finish session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                channel.future.completeExceptionally(e);
            } else {
                LOG.info("Finish session connected to worker {}({})",
                         channel.workerId, channel.client.remoteAddress());
                channel.future.complete(null);
            }
        });
    }

    private void handleDataMessage(WorkerChannel channel,
                                   SortedBufferMessage message) throws
                                   TransportException, InterruptedException {
        if (channel.client.send(message.type(), message.partitionId(),
                                message.buffer())) {
            // Pop up the element after sending successfully
            channel.queue.take();
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
        // Each target worker has a message queue
        private final SortedBufferQueue queue;
        // Each target worker has a TransportClient
        private final TransportClient client;
        private CompletableFuture<Void> future;

        public WorkerChannel(int workerId, SortedBufferQueue queue,
                             TransportClient client) {
            this.workerId = workerId;
            this.queue = queue;
            this.client = client;
            this.future = null;
        }

        public void newFuture() {
            this.future = new CompletableFuture<>();
        }
    }
}
