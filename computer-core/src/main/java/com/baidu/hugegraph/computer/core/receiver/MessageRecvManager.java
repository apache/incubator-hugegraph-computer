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
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.edge.EdgeMessageRecvPartitions;
import com.baidu.hugegraph.computer.core.receiver.message.ComputeMessageRecvPartitions;
import com.baidu.hugegraph.computer.core.receiver.message.HashIdMessageRecvPartitions;
import com.baidu.hugegraph.computer.core.receiver.vertex.VertexMessageRecvPartitions;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.computer.core.store.FileManager;
import com.baidu.hugegraph.computer.core.store.SuperstepFileGenerator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class MessageRecvManager implements Manager, MessageHandler {

    public static final String NAME = "message_recv";

    private static final Logger LOG = Log.logger(MessageRecvManager.class);

    private final ComputerContext context;
    private final FileManager fileManager;
    private final SortManager sortManager;

    private VertexMessageRecvPartitions vertexPartitions;
    private EdgeMessageRecvPartitions edgePartitions;
    private ComputeMessageRecvPartitions messagePartitions;
    private HashIdMessageRecvPartitions hashIdMessagePartitions;    

    private int workerCount;
    private int expectedFinishMessages;
    private CountDownLatch finishMessagesLatch;
    private long waitFinishMessagesTimeout;
    private long superstep;

    public MessageRecvManager(ComputerContext context,
                              FileManager fileManager,
                              SortManager sortManager) {
        this.context = context;
        this.fileManager = fileManager;
        this.sortManager = sortManager;
        this.superstep = Constants.INPUT_SUPERSTEP;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        SuperstepFileGenerator fileGenerator = new SuperstepFileGenerator(
                                               this.fileManager,
                                               Constants.INPUT_SUPERSTEP);
        this.vertexPartitions = new VertexMessageRecvPartitions(
                                this.context, fileGenerator, this.sortManager);
        this.edgePartitions = new EdgeMessageRecvPartitions(
                              this.context, fileGenerator, this.sortManager);
        this.workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
        // One for vertex and one for edge.
        this.expectedFinishMessages = this.workerCount * 2;
        this.finishMessagesLatch = new CountDownLatch(
                                   this.expectedFinishMessages);
        this.waitFinishMessagesTimeout = config.get(
             ComputerOptions.WORKER_WAIT_FINISH_MESSAGES_TIMEOUT);
    }

    @Override
    public void beforeSuperstep(Config config, int superstep) {
        SuperstepFileGenerator fileGenerator = new SuperstepFileGenerator(
                                               this.fileManager, superstep);
        this.messagePartitions = new ComputeMessageRecvPartitions(
                                 this.context, fileGenerator, this.sortManager);
        this.hashIdMessagePartitions = new HashIdMessageRecvPartitions(
                                 this.context, fileGenerator, this.sortManager);
        this.expectedFinishMessages = this.workerCount;
        this.finishMessagesLatch = new CountDownLatch(
                                   this.expectedFinishMessages);
        this.superstep = superstep;
    }

    @Override
    public void afterSuperstep(Config config, int superstep) {
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

    public void waitReceivedAllMessages() {
        try {
            boolean status = this.finishMessagesLatch.await(
                             this.waitFinishMessagesTimeout,
                             TimeUnit.MILLISECONDS);
            if (!status) {
                throw new ComputerException(
                          "Expect %s finish-messages received in %s ms, " +
                          "%s absence in superstep %s",
                          this.expectedFinishMessages,
                          this.waitFinishMessagesTimeout,
                          this.finishMessagesLatch.getCount(),
                          this.superstep);
            }
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Thread is interrupted while waiting %s " +
                      "finish-messages received in %s ms, " +
                      "%s absence in superstep %s",
                      e,
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
            case HASHID:
                System.out.printf("\n\n\n recvManager HashID \n\n\n");
                this.hashIdMessagePartitions.addBuffer(partition, buffer);
                System.out.printf("\n\n ttttt \n\n");
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
    public Map<Integer, PeekableIterator<KvEntry>> vertexPartitions() {
        E.checkState(this.vertexPartitions != null,
                     "The vertexPartitions can't be null");
        VertexMessageRecvPartitions partitions = this.vertexPartitions;
        this.vertexPartitions = null;
        return partitions.iterators();
    }

    public Map<Integer, PeekableIterator<KvEntry>> edgePartitions() {
        E.checkState(this.edgePartitions != null,
                     "The edgePartitions can't be null");
        EdgeMessageRecvPartitions partitions = this.edgePartitions;
        this.edgePartitions = null;
        return partitions.iterators();
    }

    public Map<Integer, PeekableIterator<KvEntry>> messagePartitions() {
        E.checkState(this.messagePartitions != null,
                     "The messagePartitions can't be null");
        ComputeMessageRecvPartitions partitions = this.messagePartitions;
        this.messagePartitions = null;
        return partitions.iterators();
    }

    public Map<Integer, PeekableIterator<KvEntry>> hashIdMessagePartitions() {
        E.checkState(this.hashIdMessagePartitions != null,
                     "The messagePartitions can't be null");
        HashIdMessageRecvPartitions partitions = this.hashIdMessagePartitions;
        this.hashIdMessagePartitions = null;
        return partitions.iterators();
    }

    public Map<Integer, MessageStat> messageStats() {
        this.waitReceivedAllMessages();
        E.checkState(this.messagePartitions != null,
                     "The messagePartitions can't be null");
        return this.messagePartitions.messageStats();
    }
}
