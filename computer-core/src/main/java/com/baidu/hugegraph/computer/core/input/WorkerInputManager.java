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

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.rpc.InputSplitRpcService;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.util.E;

public class WorkerInputManager implements Manager {

    public static final String NAME = "worker_input";

    /*
     * InputGraphFetcher include:
     *   VertexFetcher vertexFetcher;
     *   EdgeFetcher edgeFetcher;
     */
    private GraphFetcher fetcher;
    /*
     * Service proxy on the client
     */
    private InputSplitRpcService service;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        assert this.service != null;
        this.fetcher = InputSourceFactory.createGraphFetcher(config,
                                                             this.service);
    }

    @Override
    public void close(Config config) {
        this.fetcher.close();
    }

    public void service(InputSplitRpcService service) {
        E.checkNotNull(service, "service");
        this.service = service;
    }

    public void loadGraph() {
        // TODO: calls LoadService to load vertices and edges parallel
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
