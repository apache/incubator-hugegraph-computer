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

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.computer.core.store.SuperstepFileGenerator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;

public abstract class MessageRecvPartitions<P extends MessageRecvPartition> {

    protected final ComputerContext context;
    protected final Config config;
    protected final SuperstepFileGenerator fileGenerator;
    protected final SortManager sortManager;

    // The map of partition-id and the messages for the partition.
    private final Map<Integer, P> partitions;

    public MessageRecvPartitions(ComputerContext context,
                                 SuperstepFileGenerator fileGenerator,
                                 SortManager sortManager) {
        this.context = context;
        this.config = context.config();
        this.fileGenerator = fileGenerator;
        this.sortManager = sortManager;
        this.partitions = new HashMap<>();
    }

    protected abstract P createPartition();

    public void addBuffer(int partitionId, ManagedBuffer buffer) {
        P partition = this.partition(partitionId);
        partition.addBuffer(buffer);
    }

    private P partition(int partitionId) {
        P partition = this.partitions.get(partitionId);
        if (partition == null) {
            synchronized (this.partitions) {
                /*
                 * Not use putIfAbsent because it may create useless partition
                 * object.
                 */
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

    public Map<Integer, RecvMessageStat> recvMessageStats() {
        Map<Integer, RecvMessageStat> entries = new HashMap<>();
        for (Map.Entry<Integer, P> entry : this.partitions.entrySet()) {
            entries.put(entry.getKey(), entry.getValue().recvMessageStat());
        }
        return entries;
    }
}
