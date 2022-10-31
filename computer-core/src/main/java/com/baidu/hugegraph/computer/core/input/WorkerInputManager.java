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

package com.baidu.hugegraph.computer.core.input;

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.rpc.InputSplitRpcService;
import com.baidu.hugegraph.computer.core.sender.MessageSendManager;
import com.baidu.hugegraph.computer.core.worker.load.LoadService;

public class WorkerInputManager implements Manager {

    public static final String NAME = "worker_input";

    /*
     * Fetch vertices and edges from the data source and convert them
     * to computer-vertices and computer-edges
     */
    private final LoadService loadService;
    /*
     * Send vertex/edge or message to target worker
     */
    private final MessageSendManager sendManager;

    public WorkerInputManager(ComputerContext context,
                              MessageSendManager sendManager) {
        this.loadService = new LoadService(context);
        this.sendManager = sendManager;
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
    }

    public void service(InputSplitRpcService rpcService) {
        this.loadService.rpcService(rpcService);
    }

    /**
     * TODO: Load vertices and edges parallel.
     * When this method finish, it means that all vertices and edges are sent,
     * but there is no guarantee that all of them has been received.
     */
    public void loadGraph() {
        this.sendManager.startSend(MessageType.VERTEX);
        Iterator<Vertex> iterator = this.loadService.createIteratorFromVertex();
        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            this.sendManager.sendVertex(vertex);
        }
        this.sendManager.finishSend(MessageType.VERTEX);

        this.sendManager.startSend(MessageType.EDGE);
        iterator = this.loadService.createIteratorFromEdge();
        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            this.sendManager.sendEdge(vertex);
        }
        this.sendManager.finishSend(MessageType.EDGE);
        this.sendManager.clearBuffer();
    }
}
