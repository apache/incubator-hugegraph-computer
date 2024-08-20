/*
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

package org.apache.hugegraph.computer.core.receiver;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.snapshot.SnapshotManager;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.SuperstepFileGenerator;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;

public abstract class MessageRecvPartitions<P extends MessageRecvPartition> {

    protected final ComputerContext context;
    protected final Config config;
    protected final SuperstepFileGenerator fileGenerator;
    protected final SortManager sortManager;
    protected final SnapshotManager snapshotManager;

    // The map of partition-id and the messages for the partition.
    private final Map<Integer, P> partitions;

    public MessageRecvPartitions(ComputerContext context,
                                 SuperstepFileGenerator fileGenerator,
                                 SortManager sortManager,
                                 SnapshotManager snapshotManager) {
        this.context = context;
        this.config = context.config();
        this.fileGenerator = fileGenerator;
        this.sortManager = sortManager;
        this.snapshotManager = snapshotManager;
        this.partitions = new HashMap<>();
    }

    protected abstract P createPartition();

    protected abstract void writePartitionSnapshot(int partitionId, List<String> outputFiles);

    public void addBuffer(int partitionId, NetworkBuffer buffer) {
        P partition = this.partition(partitionId);
        partition.addBuffer(buffer);
    }

    public String genOutputPath(int partitionId) {
        P partition = this.partition(partitionId);
        String path = partition.genOutputPath();
        new File(path).getParentFile().mkdirs();
        return path;
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
            this.writePartitionSnapshot(entry.getKey(), entry.getValue().outputFiles());
        }
        return entries;
    }

    public Map<Integer, MessageStat> messageStats() {
        Map<Integer, MessageStat> entries = new HashMap<>();
        for (Map.Entry<Integer, P> entry : this.partitions.entrySet()) {
            entries.put(entry.getKey(), entry.getValue().messageStat());
        }
        return entries;
    }

    public void clearOldFiles(int superstep) {
        P partition = this.partitions.values().stream()
                                     .findFirst().orElse(null);
        if (partition != null) {
            List<String> dirs = this.fileGenerator
                                    .superstepDirs(superstep, partition.type());
            for (String dir : dirs) {
                FileUtils.deleteQuietly(new File(dir));
            }
        }
    }
}
