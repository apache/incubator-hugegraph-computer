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

package com.baidu.hugegraph.computer.core.sort.sorting;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.buffer.SortedBufferQueue;
import com.baidu.hugegraph.computer.core.buffer.SortedBufferQueuePool;
import com.baidu.hugegraph.computer.core.buffer.WriteBuffer;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.store.SortCombiner;
import com.baidu.hugegraph.computer.core.store.Sorter;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

import io.netty.buffer.ByteBufAllocator;

public class SortManager implements Manager {

    public static final Logger LOG = Log.logger(SortManager.class);

    private static final String NAME = "sort";

    private final Config config;
    private final ExecutorService sortExecutor;
    private final Sorter bufferSorter;
    private final SortCombiner valueCombiner;
    private final ByteBufAllocator allocator;
    private SortedBufferQueuePool queuePool;

    public SortManager(ComputerContext context) {
        this.config = context.config();
        this.sortExecutor = ExecutorUtil.newFixedThreadPool(4, "sort");
        // TODO：How to create
        this.bufferSorter = null;
        this.valueCombiner = null;
        this.allocator = ByteBufAllocator.DEFAULT;
        this.queuePool = null;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        Manager.super.init(config);
    }

    @Override
    public void close(Config config) {
        Manager.super.close(config);
    }

    public void bufferQueuePool(SortedBufferQueuePool bufferQueuePool) {
        this.queuePool = bufferQueuePool;
    }

    public CompletableFuture<Void> sort(int workerId, int partitionId,
                                        MessageType type, WriteBuffer buffer) {
        int capacity = this.config.get(ComputerOptions.WRITE_BUFFER_CAPACITY);
        return CompletableFuture.supplyAsync(() -> {
            RandomAccessInput bufferForRead = buffer.wrapForRead();
            // TODO：This ByteBuffer should be allocated from the off-heap
//            ByteBuf sortedBuffer = this.allocator.directBuffer(WRITE_BUFFER_CAPACITY);
            UnsafeBytesOutput sortedBuffer = new UnsafeBytesOutput(capacity);
            this.bufferSorter.sortBuffer(bufferForRead, this.valueCombiner,
                                         sortedBuffer);
            return ByteBuffer.wrap(sortedBuffer.buffer());
        }, this.sortExecutor).thenAccept(sortedBuffer -> {
            // The following code is also executed in sort thread
            buffer.finishSorting();
            // Each target worker has a buffer queue
            SortedBufferQueue queue = this.queuePool.getOrCreateQueue(workerId);
            queue.offer(partitionId, type, sortedBuffer);
        });
    }
}
