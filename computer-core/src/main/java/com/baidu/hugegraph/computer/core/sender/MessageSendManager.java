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
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.partition.HashPartitioner;
import com.baidu.hugegraph.computer.core.graph.partition.Partitioner;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.util.Log;

public class MessageSendManager implements Manager {

    public static final Logger LOG = Log.logger(MessageSendManager.class);

    public static final String NAME = "message_send";

    private final MessageSendBuffers buffers;
    private final Partitioner partitioner;
    private final SortManager sortManager;
    private final QueuedSender sender;
    private final AtomicReference<Throwable> exception;

    public MessageSendManager(ComputerContext context, SortManager sortManager,
                              QueuedSender sender) {
        this.buffers = new MessageSendBuffers(context);
        this.partitioner = new HashPartitioner();
        // The sort and client manager is inited at WorkerService init()
        this.sortManager = sortManager;
        this.sender = sender;
        this.exception = new AtomicReference<>();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        this.partitioner.init(config);
    }

    @Override
    public void close(Config config) {
        // pass
    }

    /**
     * There will multiple threads calling the method
     */
    public void sendVertex(MessageType type, Vertex vertex) {
        this.checkException();

        int partitionId = this.partitioner.partitionId(vertex.id());
        WriteBuffers buffer = this.buffers.get(partitionId);
        if (buffer.reachThreshold()) {
            // After switch, the buffer can be continued write
            buffer.switchForSorting();
            this.sortThenSend(partitionId, type, buffer);
        }
        try {
            // Write vertex to buffer
            buffer.writeVertex(type, vertex);
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to write vertex(MessageType=%s)", e, type);
        }
    }

    public void sendMessage(Id targetId, Value<?> value) {
        this.checkException();

        int partitionId = this.partitioner.partitionId(targetId);

    }

    /**
     * Start send message, put an START signal into queue
     * @param type the message type
     */
    public void startSend(MessageType type) {
        Map<Integer, WriteBuffers> all = this.buffers.all();
        try {
            for (Integer partitionId : all.keySet()) {
                int workerId = this.partitioner.workerId(partitionId);
                this.sender.send(workerId, SortedBufferMessage.START);
            }
            LOG.info("Start send message(type={})", type);
        } catch (InterruptedException e) {
            throw new ComputerException("Waiting to put buffer into " +
                                        "queue was interrupted");
        }
    }

    /**
     * Finsih send message, send the last buffer and put an END signal
     * into queue
     * @param type the message type
     */
    public void finishSend(MessageType type) {
        Map<Integer, WriteBuffers> all = this.buffers.all();
        Map<Integer, Future<?>> futures = new LinkedHashMap<>();
        // Sort and send the last buffer
        for (Map.Entry<Integer, WriteBuffers> entry : all.entrySet()) {
            int partitionId = entry.getKey();
            WriteBuffers buffer = entry.getValue();
            /*
             * If the last buffer has already been sorted and sent (empty),
             * there is no need to send again here
             */
            if (!buffer.isEmpty()) {
                buffer.prepareSorting();
                Future<?> future = this.sortThenSend(partitionId, type, buffer);
                futures.put(partitionId, future);
            }
        }
        this.checkException();

        // Wait all future finished
        try {
            for (Future<?> future : futures.values()) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new ComputerException("Failed to wait sort future finished",
                                        e);
        }
        try {
            for (Integer partitionId : all.keySet()) {
                int workerId = this.partitioner.workerId(partitionId);
                // TODO: after async send merged, wait all end ACK
                this.sender.send(workerId, SortedBufferMessage.END);
            }
            LOG.info("Finish send message(type={})", type);
        } catch (InterruptedException e) {
            throw new ComputerException("Waiting to put buffer into " +
                                        "queue was interrupted");
        }
    }

    private Future<?> sortThenSend(int partitionId, MessageType type,
                                   WriteBuffers buffer) {
        int workerId = this.partitioner.workerId(partitionId);
        return this.sortManager.sort(buffer).thenAccept(sortedBuffer -> {
            // The following code is also executed in sort thread
            buffer.finishSorting();
            // Each target worker has a buffer queue
            SortedBufferMessage message;
            message = new SortedBufferMessage(partitionId, type, sortedBuffer);
            try {
                this.sender.send(workerId, message);
            } catch (InterruptedException e) {
                throw new ComputerException("Waiting to put buffer into " +
                                                    "queue was interrupted");
            }
        }).whenComplete((r, e) -> {
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
