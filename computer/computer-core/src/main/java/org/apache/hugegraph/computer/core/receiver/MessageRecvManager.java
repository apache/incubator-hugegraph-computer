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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.MessageHandler;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.receiver.edge.EdgeMessageRecvPartitions;
import org.apache.hugegraph.computer.core.receiver.message.ComputeMessageRecvPartitions;
import org.apache.hugegraph.computer.core.receiver.vertex.VertexMessageRecvPartitions;
import org.apache.hugegraph.computer.core.snapshot.SnapshotManager;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.FileManager;
import org.apache.hugegraph.computer.core.store.SuperstepFileGenerator;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class MessageRecvManager implements Manager, MessageHandler {

    public static final String NAME = "message_recv";

    private static final Logger LOG = Log.logger(MessageRecvManager.class);

    private final ComputerContext context;
    private final FileManager fileManager;
    private final SortManager sortManager;

    private VertexMessageRecvPartitions vertexPartitions;
    private EdgeMessageRecvPartitions edgePartitions;
    private ComputeMessageRecvPartitions messagePartitions;

    private int workerCount;
    private int expectedFinishMessages;
    private CompletableFuture<Void> finishMessagesFuture;
    private AtomicInteger finishMessagesCount;
    private SnapshotManager snapshotManager;

    private long waitFinishMessagesTimeout;
    private long superstep;

    public MessageRecvManager(ComputerContext context,
                              FileManager fileManager,
                              SortManager sortManager) {
        this.context = context;
        this.fileManager = fileManager;
        this.sortManager = sortManager;
        this.superstep = Constants.INPUT_SUPERSTEP;
        this.finishMessagesCount = new AtomicInteger();
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
        this.vertexPartitions = new VertexMessageRecvPartitions(this.context,
                                                                fileGenerator,
                                                                this.sortManager,
                                                                this.snapshotManager);
        this.edgePartitions = new EdgeMessageRecvPartitions(this.context,
                                                            fileGenerator,
                                                            this.sortManager,
                                                            this.snapshotManager);
        this.workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
        // One for vertex and one for edge.
        this.expectedFinishMessages = this.workerCount * 2;
        this.finishMessagesFuture = new CompletableFuture<>();
        this.finishMessagesCount.set(this.expectedFinishMessages);

        this.waitFinishMessagesTimeout = config.get(
             ComputerOptions.WORKER_WAIT_FINISH_MESSAGES_TIMEOUT);
    }

    @Override
    public void beforeSuperstep(Config config, int superstep) {
        SuperstepFileGenerator fileGenerator = new SuperstepFileGenerator(
                                               this.fileManager, superstep);
        this.messagePartitions = new ComputeMessageRecvPartitions(this.context,
                                                                  fileGenerator,
                                                                  this.sortManager,
                                                                  this.snapshotManager);
        this.expectedFinishMessages = this.workerCount;
        this.finishMessagesFuture = new CompletableFuture<>();
        this.finishMessagesCount.set(this.expectedFinishMessages);

        this.superstep = superstep;

        if (this.superstep == Constants.INPUT_SUPERSTEP + 1) {
            assert this.vertexPartitions != null;
            this.vertexPartitions.clearOldFiles(Constants.INPUT_SUPERSTEP);
            this.vertexPartitions = null;

            assert this.edgePartitions != null;
            this.edgePartitions.clearOldFiles(Constants.INPUT_SUPERSTEP);
            this.edgePartitions = null;
        }
    }

    @Override
    public void afterSuperstep(Config config, int superstep) {
        if (superstep > Constants.INPUT_SUPERSTEP + 1) {
            this.messagePartitions.clearOldFiles(superstep - 1);
        }
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
        LOG.error("Exception caught for connection:{}, root cause:",
                 connectionId, cause);
        this.finishMessagesFuture.completeExceptionally(cause);
    }

    public void waitReceivedAllMessages() {
        try {
            this.finishMessagesFuture.get(this.waitFinishMessagesTimeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new ComputerException("Time out while waiting %s finish-messages " +
                    "received in %s ms in superstep %s",
                    this.expectedFinishMessages, this.waitFinishMessagesTimeout, this.superstep, e);
        } catch (InterruptedException | ExecutionException e) {
            throw new ComputerException("Error while waiting %s finish-messages in superstep %s",
                    this.expectedFinishMessages, this.superstep, e);
        }
    }

    @Override
    public void handle(MessageType messageType, int partition,
                       NetworkBuffer buffer) {
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
                          "Unable handle NetworkBuffer with type '%s'",
                          messageType.name());
        }
    }

    @Override
    public String genOutputPath(MessageType messageType, int partition) {
        switch (messageType) {
            case VERTEX:
                return this.vertexPartitions.genOutputPath(partition);
            case EDGE:
                return this.edgePartitions.genOutputPath(partition);
            case MSG:
                return this.messagePartitions.genOutputPath(partition);
            default:
                throw new ComputerException(
                      "Unable generator output path with type '%s'",
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
        int currentCount = this.finishMessagesCount.decrementAndGet();
        if (currentCount == 0) {
            this.finishMessagesFuture.complete(null);
        }
    }

    /**
     * Get the Iterator<KeyStore.Entry> of each partition.
     */
    public Map<Integer, PeekableIterator<KvEntry>> vertexPartitions() {
        E.checkState(this.vertexPartitions != null,
                     "The vertexPartitions can't be null");
        VertexMessageRecvPartitions partitions = this.vertexPartitions;
        return partitions.iterators();
    }

    public Map<Integer, PeekableIterator<KvEntry>> edgePartitions() {
        E.checkState(this.edgePartitions != null,
                     "The edgePartitions can't be null");
        EdgeMessageRecvPartitions partitions = this.edgePartitions;
        return partitions.iterators();
    }

    public Map<Integer, PeekableIterator<KvEntry>> messagePartitions() {
        E.checkState(this.messagePartitions != null,
                     "The messagePartitions can't be null");
        ComputeMessageRecvPartitions partitions = this.messagePartitions;
        this.messagePartitions = null;
        return partitions.iterators();
    }

    public Map<Integer, MessageStat> messageStats() {
        this.waitReceivedAllMessages();
        E.checkState(this.messagePartitions != null,
                     "The messagePartitions can't be null");
        return this.messagePartitions.messageStats();
    }

    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }
}
