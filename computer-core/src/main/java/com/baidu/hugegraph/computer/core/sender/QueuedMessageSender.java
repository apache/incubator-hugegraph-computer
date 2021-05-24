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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.util.Log;

public class QueuedMessageSender {

    public static final Logger LOG = Log.logger(QueuedMessageSender.class);

    private final Map<Integer, WorkerChannel> workerChannels;
    // The thread used to send vertex/message, only one is enough
    private final SendExecutor sendExecutor;

    private final AtomicInteger activeClientCount;
    private final AtomicInteger emptyQueueCount;
    private final AtomicInteger busyClientCount;

    private final BarrierEvent anyQueueNotEmptyEvent;
    private final BarrierEvent anyClientNotBusyEvent;

    public QueuedMessageSender() {
        this.workerChannels = new ConcurrentHashMap<>();
        this.sendExecutor = new SendExecutor();
        this.activeClientCount = new AtomicInteger();
        this.emptyQueueCount = new AtomicInteger();
        this.busyClientCount = new AtomicInteger();
        this.anyQueueNotEmptyEvent = new BarrierEvent();
        this.anyClientNotBusyEvent = new BarrierEvent();
    }

    public void init() {
        this.sendExecutor.start();
    }

    public void close() throws InterruptedException {
        this.sendExecutor.interrupt();
        this.sendExecutor.join();
    }

    public void addWorkerClient(int workerId, TransportClient client) {
        SortedBufferQueue queue = new SortedBufferQueue(
                                  this.anyQueueNotEmptyEvent::signal);
        WorkerChannel workerChannel = new WorkerChannel(queue, client);
        this.workerChannels.put(workerId, workerChannel);
    }

    public void send(int workerId, SortedBufferMessage message)
                     throws InterruptedException {
        WorkerChannel workerChannel = this.workerChannels.get(workerId);
        if (workerChannel == null)  {
            throw new ComputerException("Invalid workerId %s", workerId);
        }
        workerChannel.queue.put(message);
    }

    public Runnable notifyNotBusy() {
        return this.anyClientNotBusyEvent::signal;
    }

    private int workerCount() {
        return this.workerChannels.size();
    }

    private class SendExecutor extends Thread {

        public SendExecutor() {
            super("send-executor");
        }

        @Override
        public void run() {
            LOG.info("Start run send exector");
            while (!this.isInterrupted()) {
                for (Integer groupId : workerChannels.keySet()) {
                    try {
                        QueuedMessageSender.this.doSend(groupId);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            LOG.info("Finish run send exector");
        }
    }

    private void doSend(int workerId) throws TransportException,
                                             InterruptedException {
        WorkerChannel workerChannel = this.workerChannels.get(workerId);
        SortedBufferQueue queue = workerChannel.queue;
        TransportClient client = workerChannel.client;
        SortedBufferMessage message = queue.peek();
        if (message == null) {
            this.handleAllQueueEmpty();
            return;
        }
        switch (message.type()) {
            case START:
                this.handleStartMessage(workerId, queue, client);
                break;
            case FINISH:
                this.handleFinishMessage(workerId, queue, client);
                break;
            default:
                this.handleDataMessage(queue, client, message);
                break;
        }
    }

    private void handleAllQueueEmpty() {
        if (this.emptyQueueCount.incrementAndGet() >= this.workerCount()) {
            /*
             * If all queues are empty (maybe the sending speed is faster
             * than the upstream write speed), the sending thread should
             * suspends itself
             */
            this.waitAnyQueueNotEmpty();
            this.emptyQueueCount.decrementAndGet();
        }
    }

    private void handleStartMessage(int workerId, SortedBufferQueue queue,
                                    TransportClient client)
                                    throws TransportException,
                                           InterruptedException {
        client.startSession();
        queue.take();
        this.activeClientCount.incrementAndGet();
        LOG.info("Start session connected to worker {}({})",
                 workerId, client.remoteAddress());
    }

    private void handleFinishMessage(int workerId, SortedBufferQueue queue,
                                     TransportClient client)
                                     throws TransportException,
                                            InterruptedException {
        client.finishSession();
        queue.take();
        this.activeClientCount.decrementAndGet();
        LOG.info("Finish session connected to worker {}({})",
                 workerId, client.remoteAddress());
    }

    private void handleDataMessage(SortedBufferQueue queue,
                                   TransportClient client,
                                   SortedBufferMessage message)
                                   throws TransportException,
                                          InterruptedException {
        if (client.send(message.type(), message.partitionId(),
                        message.buffer())) {
            // Pop up the element after sending successfully
            queue.take();
        } else {
            if (this.busyClientCount.incrementAndGet() == this.workerCount()) {
                /*
                 * If all clients are busy, let send thread wait
                 * until any client is available
                 */
                this.waitAnyClientNotBusy();
                this.busyClientCount.decrementAndGet();
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
            if (this.activeClientCount.get() > 0) {
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

        // Each target worker has a message queue
        private final SortedBufferQueue queue;
        // Each target worker has a TransportClient
        private final TransportClient client;

        public WorkerChannel(SortedBufferQueue queue, TransportClient client) {
            this.queue = queue;
            this.client = client;
        }
    }
}
