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

package com.baidu.hugegraph.computer.algorithm.degreecentrality;

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.NullValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerContext;

public class DegreeCentrality implements Computation<NullValue> {

    @Override
    public String name() {
        return "degreeCentrality";
    }

    @Override
    public String category() {
        return "centrality";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        long normalization = context.totalVertexCount() - 1;
        if (normalization <= 0) {
            vertex.value(new DoubleValue(1.0));
        } else {
            vertex.value(new DoubleValue(vertex.numEdges() / normalization));
        }

        vertex.inactivate();
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<NullValue> messages) {
        // pass
    }

    @Override
    public void init(Config config) {
        // pass
    }

    @Override
    public void close(Config config) {
        // pass
    }

    @Override
    public void beforeSuperstep(WorkerContext context) {
        // pass
    }

    @Override
    public void afterSuperstep(WorkerContext context) {
        // pass
    }
}
