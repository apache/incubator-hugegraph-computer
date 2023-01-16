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

package org.apache.hugegraph.computer.core.compute;

import java.util.Iterator;
import java.util.Random;

import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;
import org.junit.Assert;

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
        checkEdgesSize(edges);
        checkEdgesSize(edges);
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

    private static void checkEdgesSize(Edges edges) {
        int edgeSize = edges.size();
        int edgeIterSize = 0;
        for (@SuppressWarnings("unused") Edge edge : edges) {
            edgeIterSize++;
        }
        Assert.assertEquals(edgeSize, edgeIterSize);
    }
}
