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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.sender.WriteBuffers;
import com.baidu.hugegraph.computer.core.store.SortCombiner;
import com.baidu.hugegraph.computer.core.store.Sorter;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

import io.netty.buffer.ByteBufAllocator;

public class SortManager implements Manager {

    public static final Logger LOG = Log.logger(SortManager.class);

    private static final String NAME = "sort";
    private static final String PREFIX = "sort-executor-";

    private final Config config;
    private final ExecutorService sortExecutor;
    private final Sorter bufferSorter;
    private final SortCombiner valueCombiner;
    private final ByteBufAllocator allocator;

    public SortManager(ComputerContext context) {
        this.config = context.config();
        int threadNum = this.config.get(ComputerOptions.SORT_THREAD_NUMS);
        this.sortExecutor = ExecutorUtil.newFixedThreadPool(threadNum, PREFIX);
        // TODO：After sort module merged, remove FakeBufferSorter
        this.bufferSorter = new FakeBufferSorter();
        this.valueCombiner = null;
        this.allocator = ByteBufAllocator.DEFAULT;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        // pass
    }

    @Override
    public void close(Config config) {
        this.sortExecutor.shutdown();
        try {
            this.sortExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Waiting sort executor terminated was interrupted");
        }
    }

    public CompletableFuture<ByteBuffer> sort(WriteBuffers buffer) {
        int capacity = this.config.get(ComputerOptions.WRITE_BUFFER_CAPACITY);
        return CompletableFuture.supplyAsync(() -> {
            RandomAccessInput bufferForRead = buffer.wrapForRead();
            // TODO：This ByteBuffer should be allocated from the off-heap
//            ByteBuf sortedBuffer = this.allocator.directBuffer(capacity);
            UnsafeBytesOutput sortedBuffer = new UnsafeBytesOutput(capacity);
            try {
                this.bufferSorter.sortBuffer(bufferForRead, this.valueCombiner,
                                             sortedBuffer);
            } catch (IOException e) {
                throw new ComputerException("Failed to sort buffer", e);
            }
            return ByteBuffer.wrap(sortedBuffer.buffer());
        }, this.sortExecutor);
    }
}
