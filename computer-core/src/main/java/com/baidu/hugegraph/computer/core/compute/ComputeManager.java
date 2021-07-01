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

package com.baidu.hugegraph.computer.core.compute;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvManager;
import com.baidu.hugegraph.computer.core.receiver.RecvStat;
import com.baidu.hugegraph.computer.core.sender.MessageSendManager;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.util.Log;

public class ComputeManager<M extends Value<M>> {

    private static final Logger LOG = Log.logger(ComputeManager.class);

    private final ComputerContext context;
    private final Managers managers;

    private final Map<Integer, FileGraphPartition<M>> partitions;
    private final Computation<M> computation;
    private final MessageRecvManager recvManager;
    private final MessageSendManager sendManager;

    public ComputeManager(ComputerContext context, Managers managers,
                          Computation<M> computation) {
        this.context = context;
        this.managers = managers;
        this.computation = computation;
        this.partitions = new HashMap<>();
        this.recvManager = this.managers.get(MessageRecvManager.NAME);
        this.sendManager = this.managers.get(MessageSendManager.NAME);
    }

    public WorkerStat input() {
        WorkerStat workerStat = new WorkerStat();
        this.recvManager.waitReceivedAllMessages();
        Map<Integer, PeekableIterator<KvEntry>> vertices;
        vertices = this.recvManager.vertexPartitions();
        Map<Integer, PeekableIterator<KvEntry>> edges;
        edges = this.recvManager.edgePartitions();
        // TODO: parallel input process
        for (Map.Entry<Integer, PeekableIterator<KvEntry>> entry :
             vertices.entrySet()) {
            FileGraphPartition<M> partition = new FileGraphPartition<>(
                                              this.context, this.managers,
                                              entry.getKey());
            PartitionStat partitionStat = partition.init(entry.getValue(),
                                          edges.get(entry.getKey()));
            workerStat.add(partitionStat);
            this.partitions.put(entry.getKey(), partition);
        }
        return workerStat;
    }

    /**
     * Get compute-messages from MessageRecvManager. Be called before
     * {@link MessageRecvManager#beforeSuperstep} is called.
     */
    public void takeComputeMessages() {
        Map<Integer, PeekableIterator<KvEntry>> messages =
                     this.recvManager.messagePartitions();
        for (FileGraphPartition<M> partition : this.partitions.values()) {
            partition.messages(messages.get(partition.partition()));
        }
    }

    public WorkerStat compute(ComputationContext context, int superstep) {
        this.sendManager.startSend(MessageType.MSG);
        WorkerStat workerStat = new WorkerStat();
        Map<Integer, PartitionStat> partitionStats = new HashMap<>(
                                                     this.partitions.size());
        if (superstep == 0) {
            // TODO: parallel compute process.
            for (FileGraphPartition<M> partition : this.partitions.values()) {
                PartitionStat stat = partition.compute0(context,
                                                        this.computation);
                partitionStats.put(stat.partitionId(), stat);
            }
        } else {
            // TODO: parallel compute process.
            for (FileGraphPartition<M> partition : this.partitions.values()) {
                PartitionStat stat = partition.compute(context,
                                                       this.computation,
                                                       superstep);
                partitionStats.put(stat.partitionId(), stat);
            }
        }
        this.sendManager.finishSend(MessageType.MSG);
        // After compute and send finish signal.
        Map<Integer, RecvStat> recvStats = this.recvManager.recvStats();
        for (Map.Entry<Integer, PartitionStat> entry :
                                               partitionStats.entrySet()) {
            PartitionStat partitionStat = entry.getValue();
            partitionStat.merge(recvStats.get(partitionStat.partitionId()));
            workerStat.add(partitionStat);
        }
        return workerStat;
    }

    public void output() {
        // TODO: Write results back parallel
        for (FileGraphPartition<M> partition : this.partitions.values()) {
            PartitionStat stat = partition.output();
            LOG.info("Output partition {} complete, stat='{}'",
                     partition.partition(), stat);
        }
    }
}
