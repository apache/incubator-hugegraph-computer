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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvManager;
import com.baidu.hugegraph.computer.core.receiver.MessageStat;
import com.baidu.hugegraph.computer.core.sender.MessageSendManager;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

public class ComputeManager {

    private static final Logger LOG = Log.logger(ComputeManager.class);
    private static final String PREFIX = "partition-compute-executor-%s";

    private final ComputerContext context;
    private final Managers managers;

    private final Map<Integer, FileGraphPartition> partitions;
    private final MessageRecvManager recvManager;
    private final MessageSendManager sendManager;
    private final ExecutorService computeExecutor;

    public ComputeManager(ComputerContext context, Managers managers) {
        this.context = context;
        this.managers = managers;
        this.partitions = new HashMap<>();
        this.recvManager = this.managers.get(MessageRecvManager.NAME);
        this.sendManager = this.managers.get(MessageSendManager.NAME);
        this.computeExecutor = ExecutorUtil.newFixedThreadPool(
                               this.threadNum(context.config()), PREFIX);
    }

    private Integer threadNum(Config config) {
        return config.get(ComputerOptions.PARTITIONS_COMPUTE_THREAD_NUMS);
    }

    public WorkerStat input() {
        WorkerStat workerStat = new WorkerStat();
        this.recvManager.waitReceivedAllMessages();

        Map<Integer, PeekableIterator<KvEntry>> vertices =
                     this.recvManager.vertexPartitions();
        Map<Integer, PeekableIterator<KvEntry>> edges =
                     this.recvManager.edgePartitions();
        // TODO: parallel input process
        for (Map.Entry<Integer, PeekableIterator<KvEntry>> entry :
             vertices.entrySet()) {
            int partition = entry.getKey();
            PeekableIterator<KvEntry> vertexIter = entry.getValue();
            PeekableIterator<KvEntry> edgesIter =
                                      edges.getOrDefault(
                                            partition,
                                            PeekableIterator.emptyIterator());

            FileGraphPartition part = new FileGraphPartition(this.context,
                                                             this.managers,
                                                             partition);
            PartitionStat partitionStat = null;
            ComputerException inputException = null;
            try {
                partitionStat = part.input(vertexIter, edgesIter);
            } catch (ComputerException e) {
                inputException = e;
            } finally {
                try {
                    vertexIter.close();
                    edgesIter.close();
                } catch (Exception e) {
                    String message = "Failed to close vertex or edge file " +
                                     "iterator";
                    ComputerException closeException = new ComputerException(
                                                           message, e);
                    if (inputException != null) {
                        inputException.addSuppressed(closeException);
                    } else {
                        throw closeException;
                    }
                }
                if (inputException != null) {
                    throw inputException;
                }
            }

            workerStat.add(partitionStat);
            this.partitions.put(partition, part);
        }
        return workerStat;
    }

    /**
     * Get compute-messages from MessageRecvManager, then put message to
     * corresponding partition. Be called before
     * {@link MessageRecvManager#beforeSuperstep} is called.
     */
    public void takeRecvedMessages() {
        Map<Integer, PeekableIterator<KvEntry>> messages =
                     this.recvManager.messagePartitions();
        for (FileGraphPartition partition : this.partitions.values()) {
            partition.messages(messages.get(partition.partition()));
        }
    }

    public WorkerStat compute(ComputationContext context, int superstep) {
        this.sendManager.startSend(MessageType.MSG);

        WorkerStat workerStat = new WorkerStat();
        Map<Integer, PartitionStat> stats = new ConcurrentHashMap<>();

        Thread thread = Thread.currentThread();
        AtomicReference<ComputerException> exception = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(this.partitions.size());

        for (FileGraphPartition partition : this.partitions.values()) {
            CompletableFuture.supplyAsync(() -> {
                return partition.compute(context, superstep);
            }, this.computeExecutor).whenComplete((stat, t) -> {
                if (t != null) {
                    ComputerException e = new ComputerException(
                                          "Partition %s compute exception ", t,
                                          partition.partition());
                    exception.compareAndSet(null, e);
                    thread.interrupt();
                }
                stats.put(stat.partitionId(), stat);
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            assert exception.get() != null;
            throw exception.get();
        }

        this.sendManager.finishSend(MessageType.MSG);

        // After compute and send finish signal.
        Map<Integer, MessageStat> recvStats = this.recvManager.messageStats();
        for (Map.Entry<Integer, PartitionStat> entry : stats.entrySet()) {
            PartitionStat partStat = entry.getValue();
            int partitionId = partStat.partitionId();

            MessageStat sendStat = this.sendManager.messageStat(partitionId);
            partStat.mergeSendMessageStat(sendStat);

            MessageStat recvStat = recvStats.get(partitionId);
            if (recvStat != null) {
                partStat.mergeRecvMessageStat(recvStat);
            }

            workerStat.add(partStat);
        }
        return workerStat;
    }

    public void output() {
        // TODO: Write results back parallel
        for (FileGraphPartition partition : this.partitions.values()) {
            PartitionStat stat = partition.output();
            LOG.info("Output partition {} complete, stat='{}'",
                     partition.partition(), stat);
        }
    }
}
