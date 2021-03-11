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

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.hg.HugeEdgeFetcher;
import com.baidu.hugegraph.computer.core.input.hg.HugeVertexFetcher;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;

public class MockWorkerInputHandler {

    private final MockRpcClient rpcClient;
    private final VertexFetcher vertexFetcher;
    private final EdgeFetcher edgeFetcher;
    private InputSplit vertexInputSplit;
    private InputSplit edgeInputSplit;

    public MockWorkerInputHandler(Config config, MockRpcClient rpcClient,
                                  HugeClient client) {
        this.rpcClient = rpcClient;
        this.vertexFetcher = new HugeVertexFetcher(config, client);
        this.edgeFetcher = new HugeEdgeFetcher(config, client);
        this.vertexInputSplit = null;
        this.edgeInputSplit = null;
    }

    public boolean fetchNextVertexInputSplit() {
        // Send request to master
        this.vertexInputSplit = this.rpcClient.getNextVertexInputSplit();
        return this.vertexInputSplit != null &&
               !this.vertexInputSplit.equals(InputSplit.END_SPLIT);
    }

    public int loadVertexInputSplitData() {
        if (this.vertexInputSplit == null) {
            throw new ComputerException("Should fetch vertex input split " +
                                        "meta before load");
        }
        if (this.vertexInputSplit.equals(InputSplit.END_SPLIT)) {
            throw new ComputerException("Can't load vertex input split data, " +
                                        "because it has been exhausted");
        }
        this.vertexFetcher.prepareLoadInputSplit(this.vertexInputSplit);
        int count = 0;
        while (this.vertexFetcher.hasNext()) {
            Vertex vertex = this.vertexFetcher.next();
            // Write vertex to buffer
            Assert.assertNotNull(vertex);
            count++;
        }
        return count;
    }

    public boolean fetchNextEdgeInputSplit() {
        // Send request to master
        this.edgeInputSplit = this.rpcClient.getNextEdgeInputSplit();
        return this.edgeInputSplit != null &&
               !this.edgeInputSplit.equals(InputSplit.END_SPLIT);
    }

    public int loadEdgeInputSplitData() {
        if (this.edgeInputSplit == null) {
            throw new ComputerException("Should fetch edge input split meta " +
                                        "before load");
        }
        if (this.edgeInputSplit.equals(InputSplit.END_SPLIT)) {
            throw new ComputerException("Can't load edge input split data, " +
                                        "because it has been exhausted");
        }
        this.edgeFetcher.prepareLoadInputSplit(this.edgeInputSplit);
        int count = 0;
        while (this.edgeFetcher.hasNext()) {
            Edge edge = this.edgeFetcher.next();
            // Write edge to buffer
            Assert.assertNotNull(edge);
            count++;
        }
        return count;
    }
}
