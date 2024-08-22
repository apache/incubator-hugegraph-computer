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

package org.apache.hugegraph.computer.core.input;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.rpc.InputSplitRpcService;
import org.apache.hugegraph.computer.core.sender.MessageSendManager;
import org.apache.hugegraph.computer.core.snapshot.SnapshotManager;
import org.apache.hugegraph.computer.core.worker.load.LoadService;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class WorkerInputManager implements Manager {

    private static final Logger LOG = Log.logger(WorkerInputManager.class);
    private static final String PREFIX = "input-send-executor-%s";

    public static final String NAME = "worker_input";

    /*
     * Fetch vertices and edges from the data source and convert them
     * to computer-vertices and computer-edges
     */
    private final LoadService loadService;

    private final ExecutorService sendExecutor;
    private final int sendThreadNum;

    /*
     * Send vertex/edge or message to target worker
     */
    private final MessageSendManager sendManager;

    private final SnapshotManager snapshotManager;

    public WorkerInputManager(ComputerContext context,
                              MessageSendManager sendManager,
                              SnapshotManager snapshotManager) {
        this.sendManager = sendManager;
        this.snapshotManager = snapshotManager;

        this.sendThreadNum = this.inputSendThreadNum(context.config());
        this.sendExecutor = ExecutorUtil.newFixedThreadPool(this.sendThreadNum, PREFIX);
        LOG.info("Created parallel sending thread pool, thread num: {}",
                sendThreadNum);

        this.loadService = new LoadService(context);
    }

    private Integer inputSendThreadNum(Config config) {
        return config.get(ComputerOptions.INPUT_SEND_THREAD_NUMS);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        this.loadService.init();
        this.sendManager.init(config);
    }

    @Override
    public void close(Config config) {
        this.loadService.close();
        this.sendManager.close(config);
        this.sendExecutor.shutdown();
    }

    public void service(InputSplitRpcService rpcService) {
        this.loadService.rpcService(rpcService);
    }

    /**
     * When this method finish, it means that all vertices and edges are sent,
     * but there is no guarantee that all of them has been received.
     */
    public void loadGraph() {
        if (this.snapshotManager.loadSnapshot()) {
            this.snapshotManager.load();
            return;
        }

        List<CompletableFuture<?>> futures = new ArrayList<>();
        CompletableFuture<?> future;
        this.sendManager.startSend(MessageType.VERTEX);
        for (int i = 0; i < this.sendThreadNum; i++) {
            future = this.send(this.sendManager::sendVertex,
                               this.loadService::createIteratorFromVertex);
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).exceptionally(e -> {
            throw new ComputerException("An exception occurred during parallel " +
                                        "sending vertices", e);
        }).join();
        this.sendManager.finishSend(MessageType.VERTEX);

        this.sendManager.startSend(MessageType.EDGE);
        futures.clear();
        for (int i = 0; i < this.sendThreadNum; i++) {
            future = this.send(this.sendManager::sendEdge,
                               this.loadService::createIteratorFromEdge);
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).exceptionally(e -> {
            throw new ComputerException("An exception occurred during parallel " +
                                        "sending edges", e);
        }).join();
        this.sendManager.finishSend(MessageType.EDGE);
        this.sendManager.clearBuffer();
    }

    private CompletableFuture<?> send(Consumer<Vertex> sendConsumer,
                                      Supplier<Iterator<Vertex>> iteratorSupplier) {
        return CompletableFuture.runAsync(() -> {
            Iterator<Vertex> iterator = iteratorSupplier.get();
            while (iterator.hasNext()) {
                Vertex vertex = iterator.next();
                sendConsumer.accept(vertex);
            }
        }, this.sendExecutor);
    }
}
