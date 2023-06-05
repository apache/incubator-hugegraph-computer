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

package org.apache.hugegraph.computer.core.sender;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.partition.Partitioner;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.receiver.MessageStat;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class MessageSendManager implements Manager {

    public static final Logger LOG = Log.logger(MessageSendManager.class);

    public static final String NAME = "message_send";

    private final MessageSendBuffers buffers;
    private final Partitioner partitioner;
    private final SortManager sortManager;
    private final MessageSender sender;
    private final AtomicReference<Throwable> exception;
    private final TransportConf transportConf;

    public MessageSendManager(ComputerContext context, SortManager sortManager,
                              MessageSender sender) {
        this.buffers = new MessageSendBuffers(context);
        this.partitioner = context.config().createObject(
                ComputerOptions.WORKER_PARTITIONER);
        this.transportConf = TransportConf.wrapConfig(context.config());
        // The sort and client manager is inited at WorkerService init()
        this.sortManager = sortManager;
        this.sender = sender;
        this.exception = new AtomicReference<>();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        this.partitioner.init(config);
    }

    @Override
    public void close(Config config) {
        // pass
    }

    /**
     * There will multiple threads calling the method
     */
    public void sendVertex(Vertex vertex) {
        this.checkException();

        int partitionId = this.partitioner.partitionId(vertex.id());
        WriteBuffers buffer = this.buffers.get(partitionId);

        this.sortIfTargetBufferIsFull(buffer, partitionId,
                                      MessageType.VERTEX);
        try {
            // Write vertex to buffer
            buffer.writeVertex(vertex);
        } catch (IOException e) {
            throw new ComputerException("Failed to write vertex '%s'",
                                        e, vertex.id());
        }
    }

    public void sendEdge(Vertex vertex) {
        this.checkException();

        int partitionId = this.partitioner.partitionId(vertex.id());
        WriteBuffers buffer = this.buffers.get(partitionId);

        this.sortIfTargetBufferIsFull(buffer, partitionId,
                                      MessageType.EDGE);
        try {
            // Write edge to buffer
            buffer.writeEdges(vertex);
        } catch (IOException e) {
            throw new ComputerException("Failed to write edges of " +
                                        "vertex '%s'", e, vertex.id());
        }
    }

    public void sendMessage(Id targetId, Value value) {
        this.checkException();

        int partitionId = this.partitioner.partitionId(targetId);
        WriteBuffers buffer = this.buffers.get(partitionId);

        this.sortIfTargetBufferIsFull(buffer, partitionId, MessageType.MSG);
        try {
            // Write message to buffer
            buffer.writeMessage(targetId, value);
        } catch (IOException e) {
            throw new ComputerException("Failed to write message", e);
        }
    }

    /**
     * Start send message, put an START signal into queue
     * @param type the message type
     */
    public void startSend(MessageType type) {
        Map<Integer, MessageSendPartition> all = this.buffers.all();
        all.values().forEach(MessageSendPartition::resetMessageWritten);
        Set<Integer> workerIds = all.keySet().stream()
                                    .map(this.partitioner::workerId)
                                    .collect(Collectors.toSet());
        this.sendControlMessageToWorkers(workerIds, MessageType.START);
        LOG.info("Start sending message(type={})", type);
    }

    /**
     * Finish send message, send the last buffer and put an END signal
     * into queue
     * @param type the message type
     */
    public void finishSend(MessageType type) {
        Map<Integer, MessageSendPartition> all = this.buffers.all();
        MessageStat stat = this.sortAndSendLastBuffer(all, type);

        Set<Integer> workerIds = all.keySet().stream()
                                    .map(this.partitioner::workerId)
                                    .collect(Collectors.toSet());
        this.sendControlMessageToWorkers(workerIds, MessageType.FINISH);
        LOG.info("Finish sending message(type={},count={},bytes={})",
                 type, stat.messageCount(), stat.messageBytes());
    }

    public MessageStat messageStat(int partitionId) {
        return this.buffers.messageStat(partitionId);
    }

    public void clearBuffer() {
        this.buffers.clear();
    }

    private void sortIfTargetBufferIsFull(WriteBuffers buffer,
                                          int partitionId,
                                          MessageType type) {
        if (buffer.reachThreshold()) {
            /*
             * Switch buffers if writing buffer is full,
             * After switch, the buffer can be continued to write.
             */
            buffer.switchForSorting();
            this.sortThenSend(partitionId, type, buffer);
        }
    }

    private Future<?> sortThenSend(int partitionId, MessageType type,
                                   WriteBuffers buffer) {
        int workerId = this.partitioner.workerId(partitionId);
        return this.sortManager.sort(type, buffer).thenAccept(sortedBuffer -> {
            // The following code is also executed in sort thread
            buffer.finishSorting();
            // Each target worker has a buffer queue
            QueuedMessage message = new QueuedMessage(partitionId, type,
                                                      sortedBuffer);
            try {
                this.sender.send(workerId, message);
            } catch (InterruptedException e) {
                throw new ComputerException("Interrupted when waiting to " +
                                            "put buffer into queue");
            }
        }).whenComplete((r, e) -> {
            if (e != null) {
                LOG.error("Failed to sort buffer or put sorted buffer " +
                          "into queue", e);
                // Just record the first error
                this.exception.compareAndSet(null, e);
            }
        });
    }

    private MessageStat sortAndSendLastBuffer(
                        Map<Integer, MessageSendPartition> all,
                        MessageType type) {
        MessageStat messageWritten = new MessageStat();
        List<Future<?>> futures = new ArrayList<>(all.size());
        // Sort and send the last buffer
        for (Map.Entry<Integer, MessageSendPartition> entry : all.entrySet()) {
            int partitionId = entry.getKey();
            MessageSendPartition partition = entry.getValue();
            /*
             * If the last buffer has already been sorted and sent (empty),
             * there is no need to send again here
             */
            for (WriteBuffers buffer : partition.buffers()) {
                if (!buffer.isEmpty()) {
                    buffer.prepareSorting();
                    futures.add(this.sortThenSend(partitionId, type, buffer));
                }
            }
            // Record total message count & bytes
            messageWritten.increase(partition.messageWritten());
        }
        this.checkException();

        // Wait all future finished
        try {
            for (Future<?> future : futures) {
                future.get(Constants.FUTURE_TIMEOUT, TimeUnit.SECONDS);
            }
        } catch (TimeoutException e) {
            throw new ComputerException("Timed out to wait for sorting task " +
                                        "to finished", e);
        } catch (InterruptedException | ExecutionException e) {
            throw new ComputerException("Failed to wait for sorting task " +
                                        "to finished", e);
        }

        return messageWritten;
    }

    private void sendControlMessageToWorkers(Set<Integer> workerIds,
                                             MessageType type) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            for (Integer workerId : workerIds) {
                futures.add(this.sender.send(workerId, type));
            }
        } catch (InterruptedException e) {
            throw new ComputerException("Interrupted when waiting to " +
                                        "send message async");
        }

        long timeout = type == MessageType.FINISH ?
                       this.transportConf.timeoutFinishSession() :
                       this.transportConf.timeoutSyncRequest();
        try {
            for (CompletableFuture<Void> future : futures) {
                future.get(timeout, TimeUnit.MILLISECONDS);
            }
        } catch (TimeoutException e) {
            throw new ComputerException("Timeout(%sms) to wait for " +
                                        "controlling message(%s) to finished",
                                        e, timeout, type);
        } catch (InterruptedException | ExecutionException e) {
            throw new ComputerException("Failed to wait for controlling " +
                                        "message(%s) to finished", e, type);
        }
    }

    private void checkException() {
        if (this.exception.get() != null) {
            throw new ComputerException("Failed to send message",
                                        this.exception.get());
        }
    }
}
