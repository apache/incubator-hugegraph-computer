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

package com.baidu.hugegraph.computer.core.worker;

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.testutil.Assert;

public class MockComputation implements Computation<DoubleValue> {

    public void beforeSuperstep(WorkerContext context) {
        Assert.assertEquals(100L, context.totalVertexCount());
        Assert.assertEquals(200L, context.totalEdgeCount());
    }

    @Override
    public String name() {
        return "mock";
    }

    @Override
    public String category() {
        return "mock";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        // pass
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<DoubleValue> messages) {
        // pass
    }
}
