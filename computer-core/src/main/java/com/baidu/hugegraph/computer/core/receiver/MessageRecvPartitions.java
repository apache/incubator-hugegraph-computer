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

package com.baidu.hugegraph.computer.core.receiver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.FileGenerator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;

public abstract class MessageRecvPartitions<P extends MessageRecvPartition> {

    protected final ComputerContext context;
    protected final Config config;
    protected final FileGenerator fileGenerator;
    protected final Sorter sorter;
    protected final int superstep;

    private final Map<Integer, P> partitions;

    public MessageRecvPartitions(ComputerContext context,
                                 FileGenerator fileGenerator,
                                 Sorter sorter,
                                 int superstep) {
        this.context = context;
        this.config = context.config();
        this.fileGenerator = fileGenerator;
        this.sorter = sorter;
        this.superstep = superstep;
        this.partitions = new ConcurrentHashMap<>();
    }

    public abstract P createPartition();

    public void addBuffer(int partitionId, ManagedBuffer buffer) {
        P partition = this.partition(partitionId);
        partition.addBuffer(buffer);
    }

    private P partition(int partitionId) {
        P partition = this.partitions.get(partitionId);
        if (partition == null) {
            synchronized (this.partitions) {
                partition = this.partitions.get(partitionId);
                if (partition == null) {
                    partition = this.createPartition();
                    this.partitions.put(partitionId, partition);
                }
            }
        }
        return partition;
    }

    public Map<Integer, PeekableIterator<KvEntry>> iterators() {
        Map<Integer, PeekableIterator<KvEntry>> entries = new HashMap<>();
        for (Map.Entry<Integer, P> entry : this.partitions.entrySet()) {
            entries.put(entry.getKey(), entry.getValue().iterator());
        }
        return entries;
    }
}