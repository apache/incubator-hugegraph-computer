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

import java.security.KeyStore;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.edge.EdgeMessageRecvPartitions;
import com.baidu.hugegraph.computer.core.receiver.message.ComputeMessageRecvPartitions;
import com.baidu.hugegraph.computer.core.receiver.vertex.VertexMessageRecvPartitions;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.store.FileManager;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.util.Log;

public class MessageRecvManager implements Manager, MessageHandler {

    private static final Logger LOG = Log.logger(MessageRecvManager.class);

    public static final String NAME = "message_recv";

    private Config config;
    private final FileManager fileManager;
    private ComputerContext context;

    private VertexMessageRecvPartitions vertexPartitions;
    private EdgeMessageRecvPartitions edgePartitions;
    private ComputeMessageRecvPartitions messagePartitions;

    private int workerCount;
    private int expectedFinishMessages;
    private CountDownLatch finishMessagesLatch;
    private long waitFinishMessagesTimeout;
    private long superstep;
    private Sorter sorter;


    public MessageRecvManager(FileManager fileManager,
                              ComputerContext context) {
        this.fileManager = fileManager;
        this.superstep = Constants.INPUT_SUPERSTEP;
        this.context = context;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        this.config = config;
        this.sorter = new SorterImpl(this.config);
        this.vertexPartitions = new VertexMessageRecvPartitions(
                                this.config, this.fileManager,
                                this.context, this.sorter);
        this.edgePartitions = new EdgeMessageRecvPartitions(
                              this.config, this.fileManager,
                              this.context, this.sorter);
        this.workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
        // One for vertex and one for edge.
        this.expectedFinishMessages = this.workerCount * 2;
        this.finishMessagesLatch =
        new CountDownLatch(this.expectedFinishMessages);
        this.waitFinishMessagesTimeout =
        config.get(ComputerOptions.WORKER_WAIT_FINISH_MESSAGES_TIMEOUT);
    }

    @Override
    public void beforeSuperstep(Config config, int superstep) {
        this.messagePartitions = new ComputeMessageRecvPartitions(
                                 this.config, this.fileManager, this.sorter);
        this.expectedFinishMessages = this.workerCount;
        this.finishMessagesLatch =
        new CountDownLatch(this.expectedFinishMessages);
        this.superstep = superstep;
    }

    @Override
    public void afterSuperstep(Config config, int superstep) {
        // pass
    }

    @Override
    public void onChannelActive(ConnectionId connectionId) {
        // pass
    }

    @Override
    public void onChannelInactive(ConnectionId connectionId) {
        // pass
    }

    @Override
    public void exceptionCaught(TransportException cause,
                                ConnectionId connectionId) {
        // TODO: implement failover
        LOG.warn("Exception caught for connection:{}, root cause:{}",
                 connectionId, cause);
    }

    /**
     * Merge vertices and edges in each partition parallel, and get the
     * workerStat. Be called at the end of input superstep.
     */
    public WorkerStat mergeGraph() {
        // TODO: Merge vertices and edges in each partition parallel
        PartitionStat stat1 = new PartitionStat(0, 100L, 200L,
                                                50L, 60L, 70L);
        WorkerStat workerStat = new WorkerStat();
        workerStat.add(stat1);
        return workerStat;
    }

    public void waitReceivedAllMessages() {
        try {
            this.finishMessagesLatch.await(this.waitFinishMessagesTimeout,
                                           TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Expected %s finish messages in %sms, %s absence in " +
                      "superstep %s",
                      this.expectedFinishMessages,
                      this.waitFinishMessagesTimeout,
                      this.finishMessagesLatch.getCount(),
                      this.superstep);
        }
    }

    @Override
    public void handle(MessageType messageType, int partition,
                       ManagedBuffer buffer) {
        switch (messageType) {
            case VERTEX:
                this.vertexPartitions.addBuffer(partition, buffer);
                break;
            case EDGE:
                this.edgePartitions.addBuffer(partition, buffer);
                break;
            case MSG:
                this.messagePartitions.addBuffer(partition, buffer);
                break;
            default:
                throw new ComputerException(
                          "Unable handle ManagedBuffer with type '%s'",
                          messageType.name());
        }
    }

    @Override
    public void onStarted(ConnectionId connectionId) {
        LOG.debug("ConnectionId {} started", connectionId);
    }

    @Override
    public void onFinished(ConnectionId connectionId) {
        LOG.debug("ConnectionId {} finished", connectionId);
        this.finishMessagesLatch.countDown();
    }

    /**
     * Get the Iterator<KeyStore.Entry> of each partition.
     */
    public Map<Integer, Iterator<KeyStore.Entry>> vertexPartitions() {
        VertexMessageRecvPartitions partitions = this.vertexPartitions;
        this.vertexPartitions = null;
        return partitions.entryIterators();
    }

    public Map<Integer, Iterator<KeyStore.Entry>> edgePartitions() {
        EdgeMessageRecvPartitions partitions = this.edgePartitions;
        this.edgePartitions = null;
        return partitions.entryIterators();
    }

    public Map<Integer, Iterator<KeyStore.Entry>> messagePartitions() {
        ComputeMessageRecvPartitions partitions = this.messagePartitions;
        this.messagePartitions = null;
        return partitions.entryIterators();
    }
}
