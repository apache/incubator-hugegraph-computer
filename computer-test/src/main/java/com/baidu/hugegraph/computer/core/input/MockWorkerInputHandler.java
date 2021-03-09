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
import com.baidu.hugegraph.computer.core.input.hg.HugeEdgeFetcher;
import com.baidu.hugegraph.computer.core.input.hg.HugeVertexFetcher;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;

public class MockWorkerInputHandler {

    private final MockRpcClient rpcClient;
    private final VertexFetcher vertexFetcher;
    private final EdgeFetcher edgeFetcher;
    private InputSplit vertexInputSplit;
    private InputSplit edgeInputSplit;

    public MockWorkerInputHandler(MockRpcClient rpcClient, HugeClient client) {
        this.rpcClient = rpcClient;
        this.vertexFetcher = new HugeVertexFetcher(client);
        this.edgeFetcher = new HugeEdgeFetcher(client);
        this.vertexInputSplit = null;
        this.edgeInputSplit = null;
    }

    public boolean fetchNextVertexInputSplit() {
        // Send request to master
        this.vertexInputSplit = this.rpcClient.getNextVertexInputSplit();
        return this.vertexInputSplit != null &&
               this.vertexInputSplit != InputSplit.END_SPLIT;
    }

    public void loadVertexInputSplitData() {
        if (this.vertexInputSplit == null) {
            throw new ComputerException("Should fetch vertex input split " +
                                        "meta before load");
        }
        if (this.vertexInputSplit == InputSplit.END_SPLIT) {
            throw new ComputerException("Can't load vertex input split data, " +
                                        "because it has been exhausted");
        }
        this.vertexFetcher.prepareLoadInputSplit(this.vertexInputSplit);
        while (this.vertexFetcher.hasNext()) {
            Vertex vertex = this.vertexFetcher.next();
            // Write vertex to buffer
            System.out.println(vertex);
        }
    }

    public boolean fetchNextEdgeInputSplit() {
        // Send request to master
        this.edgeInputSplit = this.rpcClient.getNextEdgeInputSplit();
        return this.edgeInputSplit != null &&
               this.edgeInputSplit != InputSplit.END_SPLIT;
    }

    public void loadEdgeInputSplitData() {
        if (this.edgeInputSplit == null) {
            throw new ComputerException("Should fetch edge input split meta " +
                                        "before load");
        }
        if (this.edgeInputSplit == InputSplit.END_SPLIT) {
            throw new ComputerException("Can't load edge input split data, " +
                                        "because it has been exhausted");
        }
        this.edgeFetcher.prepareLoadInputSplit(this.edgeInputSplit);
        while (this.edgeFetcher.hasNext()) {
            Edge edge = this.edgeFetcher.next();
            // Write edge to buffer
            System.out.println(edge);
        }
    }
}
