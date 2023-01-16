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

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.testutil.Assert;

public class MockWorkerInputManager implements Manager {

    private final MockRpcClient rpcClient;
    private GraphFetcher fetcher;
    private InputSplit vertexInputSplit;
    private InputSplit edgeInputSplit;

    public MockWorkerInputManager(MockRpcClient rpcClient) {
        this.rpcClient = rpcClient;
        this.fetcher = null;
        this.vertexInputSplit = null;
        this.edgeInputSplit = null;
    }

    @Override
    public String name() {
        return "mock_worker_input";
    }

    @Override
    public void init(Config config) {
        this.fetcher = InputSourceFactory.createGraphFetcher(config,
                                                             this.rpcClient);
        this.vertexInputSplit = null;
        this.edgeInputSplit = null;
    }

    @Override
    public void close(Config config) {
        this.fetcher.close();
    }

    public boolean fetchNextVertexInputSplit() {
        // Send request to master
        this.vertexInputSplit = this.fetcher.nextVertexInputSplit();
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
        VertexFetcher vertexFetcher = this.fetcher.vertexFetcher();
        vertexFetcher.prepareLoadInputSplit(this.vertexInputSplit);
        int count = 0;
        while (vertexFetcher.hasNext()) {
            Vertex vertex = vertexFetcher.next();
            // Write vertex to buffer
            Assert.assertNotNull(vertex);
            count++;
        }
        return count;
    }

    public boolean fetchNextEdgeInputSplit() {
        // Send request to master
        this.edgeInputSplit = this.fetcher.nextEdgeInputSplit();
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
        EdgeFetcher edgeFetcher = this.fetcher.edgeFetcher();
        edgeFetcher.prepareLoadInputSplit(this.edgeInputSplit);
        int count = 0;
        while (edgeFetcher.hasNext()) {
            Edge edge = edgeFetcher.next();
            // Write edge to buffer
            Assert.assertNotNull(edge);
            count++;
        }
        return count;
    }
}
