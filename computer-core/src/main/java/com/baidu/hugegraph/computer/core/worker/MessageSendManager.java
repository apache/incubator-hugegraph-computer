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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.partition.HashPartitioner;
import com.baidu.hugegraph.computer.core.graph.partition.Partitioner;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.sender.MessageSendPartition;
import com.baidu.hugegraph.computer.core.sender.MessageSendPartitions;
import com.baidu.hugegraph.computer.core.sender.SortedBufferMessage;
import com.baidu.hugegraph.computer.core.sender.SortedBufferQueue;
import com.baidu.hugegraph.computer.core.sender.SortedBufferQueuePool;
import com.baidu.hugegraph.computer.core.sender.WriteBuffers;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.util.Log;

public class MessageSendManager implements Manager {

    public static final Logger LOG = Log.logger(MessageSendManager.class);

    private static final String NAME = "message_send";

    // TODO: 这个名字实在是太奇怪了，无法忍受
    private final MessageSendPartitions sendPartitions;
    private final Partitioner partitioner;
    private final SortManager sortManager;
    private final DataClientManager clientManager;
//    private final MessageSendTargetWorkers sendTargetWorkers;
    private final SortedBufferQueuePool queuePool;
    private final AtomicReference<Throwable> exception;

    public MessageSendManager(ComputerContext context,
                              SortManager sortManager,
                              DataClientManager clientManager) {
        this.sendPartitions = new MessageSendPartitions(context);
        this.partitioner = new HashPartitioner();
        this.sortManager = sortManager;
        this.clientManager = clientManager;
        this.queuePool = new SortedBufferQueuePool(context);
        this.exception = new AtomicReference<>();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        this.partitioner.init(config);
        this.sortManager.init(config);
        this.clientManager.init(config);
        // The sort manager as producer
        this.sortManager.bufferQueuePool(this.queuePool);
        // The client manager as consumer
        this.clientManager.bufferQueuePool(this.queuePool);
    }

    @Override
    public void close(Config config) {
        Manager.super.close(config);
    }

    /**
     * There will multiple threads calling the method
     */
    public void sendVertex(MessageType type, Vertex vertex) {
        this.checkException();

        int partitionId = this.partitioner.partitionId(vertex.id());
        MessageSendPartition partition = this.sendPartitions.get(partitionId);
        WriteBuffers buffer = partition.writeBuffer();
        if (buffer.reachThreshold()) {
            // After switch, the buffer can be continued write
            buffer.switchForSorting();
            this.sort(partitionId, type, buffer);
        }
        synchronized (buffer) {
            // Write vertex to buffer
            try {
                buffer.writeVertex(type, vertex);
            } catch (IOException e) {
                throw new ComputerException(
                          "Failed to write vertex(MessageType=%s)", e, type);
            }
        }
    }

    /**
     * Start send message, put an START signal into queue
     * @param type the message type
     */
    public void startSend(MessageType type) {
        Map<Integer, MessageSendPartition> all = this.sendPartitions.all();
        for (Map.Entry<Integer, MessageSendPartition> entry : all.entrySet()) {
            int partitionId = entry.getKey();
            int workerId = this.partitioner.workerId(partitionId);
            SortedBufferQueue queue = this.queuePool.get(workerId);
            queue.offer(SortedBufferMessage.START);
        }
        LOG.info("Start send message(type={})", type);
    }

    /**
     * Finsih send message, send the last buffer and put an END signal
     * into queue
     * @param type the message type
     */
    public void finishSend(MessageType type) {
        Map<Integer, MessageSendPartition> all = this.sendPartitions.all();
        Map<Integer, Future<?>> futures = new LinkedHashMap<>();
        // Sort and send the last buffer
        for (Map.Entry<Integer, MessageSendPartition> entry : all.entrySet()) {
            int partitionId = entry.getKey();
            MessageSendPartition partition = entry.getValue();
            WriteBuffers buffer = partition.writeBuffer();
            /*
             * If the last buffer has already been sorted and sent (empty),
             * there is no need to send again here
             */
            if (!buffer.isEmpty()) {
                buffer.prepareSorting();
                Future<?> future = this.sort(partitionId, type, buffer);
                futures.put(partitionId, future);
            }
        }
        this.checkException();

        // Wait all future finished
        for (Map.Entry<Integer, Future<?>> entry : futures.entrySet()) {
            int partitionId = entry.getKey();
            Future<?> future = entry.getValue();
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new ComputerException("");
            }
            // Add an END to the queue after the sorting is completed
            int workerId = this.partitioner.workerId(partitionId);
            SortedBufferQueue queue = this.queuePool.get(workerId);
            queue.offer(SortedBufferMessage.END);
        }
        LOG.info("Finish send message(type={})", type);
    }

    private Future<?> sort(int partitionId, MessageType type,
                           WriteBuffers buffer) {
        int workerId = this.partitioner.workerId(partitionId);
        return this.sortManager.sort(workerId, partitionId, type, buffer)
                        .whenComplete((r, e) -> {
            if (e != null) {
                LOG.error("Failed to sort buffer or put sorted buffer " +
                          "into queue", e);
                // Just record the first error
                this.exception.compareAndSet(null, e);
            }
        });
    }

    private void checkException() {
        if (this.exception.get() != null) {
            throw new ComputerException("Failed to send message",
                                        this.exception.get());
        }
    }
}
