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

import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hugegraph.computer.core.rpc.InputSplitRpcService;

public class MasterInputHandler implements InputSplitRpcService {

    private final InputSplitFetcher fetcher;
    private final Queue<InputSplit> vertexSplits;
    private final Queue<InputSplit> edgeSplits;

    public MasterInputHandler(InputSplitFetcher fetcher) {
        this.fetcher = fetcher;
        this.vertexSplits = new LinkedBlockingQueue<>();
        this.edgeSplits = new LinkedBlockingQueue<>();
    }

    public int createVertexInputSplits() {
        List<InputSplit> splits = this.fetcher.fetchVertexInputSplits();
        for (InputSplit split : splits) {
            this.vertexSplits.offer(split);
        }
        return this.vertexSplits.size();
    }

    public int createEdgeInputSplits() {
        List<InputSplit> splits = this.fetcher.fetchEdgeInputSplits();
        for (InputSplit split : splits) {
            this.edgeSplits.offer(split);
        }
        return this.edgeSplits.size();
    }

    @Override
    public InputSplit nextVertexInputSplit() {
        InputSplit split = this.vertexSplits.poll();
        return split != null ? split : InputSplit.END_SPLIT;
    }

    @Override
    public InputSplit nextEdgeInputSplit() {
        InputSplit split = this.edgeSplits.poll();
        return split != null ? split : InputSplit.END_SPLIT;
    }
}
