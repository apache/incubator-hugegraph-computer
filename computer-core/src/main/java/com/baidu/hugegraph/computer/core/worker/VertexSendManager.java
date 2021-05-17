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
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.buffer.SortedBufferQueuePool;
import com.baidu.hugegraph.computer.core.buffer.WriteBuffer;
import com.baidu.hugegraph.computer.core.buffer.WriteBufferPool;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.partition.HashPartitioner;
import com.baidu.hugegraph.computer.core.graph.partition.Partitioner;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.util.Log;

public class VertexSendManager implements Manager {

    public static final Logger LOG = Log.logger(VertexSendManager.class);

    private static final String NAME = "vertex_send";

    private final Partitioner partitioner;
    private final WriteBufferPool writeBufferPool;
    private final SortManager sortManager;
    private final DataClientManager clientManager;
    private final SortedBufferQueuePool bufferQueuePool;
    private final AtomicReference<Throwable> exception;

    public VertexSendManager(ComputerContext context,
                             SortManager sortManager,
                             DataClientManager clientManager) {
        this.partitioner = new HashPartitioner();
        this.writeBufferPool = new WriteBufferPool(context);
        this.sortManager = sortManager;
        this.clientManager = clientManager;
        this.bufferQueuePool = new SortedBufferQueuePool();
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
        this.sortManager.bufferQueuePool(this.bufferQueuePool);
        // The client manager as consumer
        this.clientManager.bufferQueuePool(this.bufferQueuePool);
    }

    @Override
    public void close(Config config) {
        Manager.super.close(config);
    }

    /**
     * There will multiple read threads calling the method
     */
    public synchronized void sendVertex(MessageType type, Vertex vertex) {
        if (this.exception.get() != null) {
            throw new ComputerException("Failed to send vertex(MessageType=%s)",
                      this.exception.get(), type);
        }

        int partitionId = this.partitioner.partitionId(vertex.id());
        // Each target partition has a write buffer
        WriteBuffer buffer = this.writeBufferPool.getOrCreateBuffer(
                             partitionId);
        if (buffer.reachThreshold()) {
            // After switch, the buffer can be continued write
            buffer.switchForSorting();
            int workerId = this.partitioner.workerId(partitionId);
            this.sortManager.sort(workerId, partitionId, type, buffer)
                            .whenComplete((r, e) -> {
                if (e != null) {
                    LOG.error("Failed to sort buffer or put sorted buffer " +
                              "into queue", e);
                    // Just record the first error
                    this.exception.compareAndSet(null, e);
                }
            });
        }
        // Write vertex to buffer
        try {
            buffer.writeVertex(type, vertex);
        } catch (IOException e) {
            throw new ComputerException(
                      "Failed to write vertex(MessageType=%s)", e, type);
        }
    }
}
