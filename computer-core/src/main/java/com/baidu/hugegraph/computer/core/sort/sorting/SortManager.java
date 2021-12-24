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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.combiner.EdgeValueCombiner;
import com.baidu.hugegraph.computer.core.combiner.MessageValueCombiner;
import com.baidu.hugegraph.computer.core.combiner.PointerCombiner;
import com.baidu.hugegraph.computer.core.combiner.VertexValueCombiner;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.sender.WriteBuffers;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineSubKvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.KvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

public abstract class SortManager implements Manager {

    public static final Logger LOG = Log.logger(SortManager.class);

    private final ComputerContext context;
    private final ExecutorService sortExecutor;
    private final Sorter sorter;
    private final int capacity;
    private final int flushThreshold;

    public SortManager(ComputerContext context) {
        this.context = context;
        Config config = context.config();
        this.sortExecutor = ExecutorUtil.newFixedThreadPool(
                            this.threadNum(config), this.threadPrefix());
        this.sorter = new SorterImpl(config);
        this.capacity = config.get(
                        ComputerOptions.WORKER_WRITE_BUFFER_INIT_CAPACITY);
        this.flushThreshold = config.get(
                              ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX);
    }

    @Override
    public abstract String name();

    protected abstract String threadPrefix();

    protected Integer threadNum(Config config) {
        return config.get(ComputerOptions.SORT_THREAD_NUMS);
    }

    @Override
    public void init(Config config) {
        // pass
    }

    @Override
    public void close(Config config) {
        this.sortExecutor.shutdown();
        try {
            this.sortExecutor.awaitTermination(Constants.SHUTDOWN_TIMEOUT,
                                               TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted when waiting sort executor terminated");
        }
    }

    public CompletableFuture<ByteBuffer> sort(MessageType type,
                                              WriteBuffers buffer) {
        return CompletableFuture.supplyAsync(() -> {
            RandomAccessInput bufferForRead = buffer.wrapForRead();
            // TODO：This ByteBuffer should be allocated from the off-heap
            BytesOutput output = IOFactory.createBytesOutput(this.capacity);
            InnerSortFlusher flusher = this.createSortFlusher(
                                       type, output,
                                       this.flushThreshold);
            try {
                this.sorter.sortBuffer(bufferForRead, flusher,
                                       type == MessageType.EDGE);
            } catch (Exception e) {
                throw new ComputerException("Failed to sort buffers of %s " +
                                            "message", e, type.name());
            }

            return ByteBuffer.wrap(output.buffer(), 0, (int) output.position());
        }, this.sortExecutor);
    }

    public CompletableFuture<Void> mergeBuffers(List<RandomAccessInput> inputs,
                                                String path,
                                                boolean withSubKv,
                                                OuterSortFlusher flusher) {
        return CompletableFuture.runAsync(() -> {
            if (withSubKv) {
                flusher.sources(inputs.size());
            }
            try {
                this.sorter.mergeBuffers(inputs, flusher, path, withSubKv);
            } catch (Exception e) {
                throw new ComputerException(
                          "Failed to merge %s buffers to file '%s'",
                          e, inputs.size(), path);
            }
        }, this.sortExecutor);
    }

    public void mergeInputs(List<String> inputs, List<String> outputs,
                            boolean withSubKv, OuterSortFlusher flusher) {
        if (withSubKv) {
            flusher.sources(inputs.size());
        }
        try {
            this.sorter.mergeInputs(inputs, flusher, outputs, withSubKv);
        } catch (Exception e) {
            throw new ComputerException(
                      "Failed to merge %s files into %s files",
                      e, inputs.size(), outputs.size());
        }
    }

    public PeekableIterator<KvEntry> iterator(List<String> outputs,
                                              boolean withSubKv) {
        try {
            return this.sorter.iterator(outputs, withSubKv);
        } catch (IOException e) {
            throw new ComputerException("Failed to iterate files: '%s'",
                                        outputs);
        }
    }

    private InnerSortFlusher createSortFlusher(MessageType type,
                                               RandomAccessOutput output,
                                               int flushThreshold) {
        PointerCombiner combiner;
        boolean needSortSubKv;

        switch (type) {
            case VERTEX:
                combiner = new VertexValueCombiner(this.context);
                needSortSubKv = false;
                break;
            case EDGE:
                combiner = new EdgeValueCombiner(this.context);
                needSortSubKv = true;
                break;
            case MSG:
                combiner = this.createMessageCombiner();
                needSortSubKv = false;
                break;
            default:
                throw new ComputerException("Unsupported combine message " +
                                            "type for %s", type);
        }

        InnerSortFlusher flusher;
        if (combiner == null) {
            flusher = new KvInnerSortFlusher(output);
        } else {
            if (needSortSubKv) {
                flusher = new CombineSubKvInnerSortFlusher(output, combiner,
                                                           flushThreshold);
            } else {
                flusher = new CombineKvInnerSortFlusher(output, combiner);
            }
        }

        return flusher;
    }

    private PointerCombiner createMessageCombiner() {
        Config config = this.context.config();
        Combiner<Value> valueCombiner = config.createObject(
                                        ComputerOptions.WORKER_COMBINER_CLASS,
                                        false);
        if (valueCombiner == null) {
            return null;
        }
        return new MessageValueCombiner(this.context);
    }
}
