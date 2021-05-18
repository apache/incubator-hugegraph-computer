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
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.rpc.InputSplitRpcService;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.computer.core.worker.DataClientManager;
import com.baidu.hugegraph.computer.core.worker.VertexSendManager;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.computer.core.worker.load.LoadService;

public class WorkerInputManager implements Manager {

    public static final String NAME = "worker_input";

    private final LoadService loadService;
    private final VertexSendManager sendManager;

    public WorkerInputManager(ComputerContext context, SortManager sortManager,
                              DataClientManager clientManager) {
        this.loadService = new LoadService(context);
        this.sendManager = new VertexSendManager(context, sortManager,
                                                 clientManager);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        this.loadService.init(config);
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

    public void loadGraph() {
        Iterator<Vertex> iterator = this.loadService.createIteratorFromVertex();
        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            this.sendManager.sendVertex(MessageType.VERTEX, vertex);
        }
        this.sendManager.finish(MessageType.VERTEX);

        iterator = this.loadService.createIteratorFromEdge();
        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            this.sendManager.sendVertex(MessageType.EDGE, vertex);
        }
        this.sendManager.finish(MessageType.EDGE);
    }

    public WorkerStat mergeGraph() {
        // TODO: merge the data in partitions parallel, and get workerStat
        PartitionStat stat1 = new PartitionStat(0, 100L, 200L,
                                                50L, 60L, 70L);
        WorkerStat workerStat = new WorkerStat();
        workerStat.add(stat1);
        return workerStat;
    }
}
