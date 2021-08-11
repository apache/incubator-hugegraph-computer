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

package com.baidu.hugegraph.computer.core.compute;

import java.util.Iterator;
import java.util.Random;

import org.junit.Assert;

import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;

public class MockComputation implements Computation<IdList> {

    private static final String NAME = "MockComputation";
    private static final String CATEGORY = "Mock";
    private static final Random RANDOM = new Random(1001L);

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String category() {
        return CATEGORY;
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        IdListList value = new IdListList();
        vertex.value(value);
        Edges edges =  vertex.edges();
        checkEdges(edges);
        checkEdges(edges);
        if (RANDOM.nextInt() % 10 == 0) {
            vertex.inactivate();
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<IdList> messages) {
        IdListList value = vertex.value();
        while (messages.hasNext()) {
            Assert.assertTrue(messages.hasNext());
            value.add(messages.next().copy());
        }
        Assert.assertFalse(messages.hasNext());
        if (RANDOM.nextInt() % 10 == 0) {
            vertex.inactivate();
        }
    }

    private static void checkEdges(Edges edges) {
        int edgeSize = edges.size();
        int edgeIndex = 0;
        for (Edge edge : edges) {
            edgeIndex++;
        }
        Assert.assertEquals(edgeSize, edgeIndex);
    }
}
