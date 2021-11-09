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

package com.baidu.hugegraph.computer.algorithm.community.trianglecount;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdSet;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;

public class TriangleCount implements Computation<IdList> {

    public static final String ALGORITHM_NAME = "triangle_count";

    @Override
    public String name() {
        return ALGORITHM_NAME;
    }

    @Override
    public String category() {
        return "community";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        TriangleCountValue value = new TriangleCountValue();
        IdSet allNeighbors = value.idSet();

        // Collect all neighbors, include of incoming and outgoing
        allNeighbors.addAll(getAllNeighbors(vertex));

        // Collect neighbors of id less than self from all neighbors
        IdList neighbors = new IdList();
        for (Id neighbor : allNeighbors.values()) {
            if (neighbor.compareTo(vertex.id()) < 0) {
                neighbors.add(neighbor);
            }
        }

        // Send all neighbors of id less than self to neighbors
        for (Id targetId : allNeighbors.values()) {
            context.sendMessage(targetId, neighbors);
        }

        vertex.value(value);
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<IdList> messages) {
        Integer count = this.triangleCount(context, vertex, messages);
        if (count != null) {
            ((TriangleCountValue) vertex.value()).count(count);
            vertex.inactivate();
        }
    }

    private Integer triangleCount(ComputationContext context, Vertex vertex,
                                  Iterator<IdList> messages) {
        IdSet allNeighbors = ((TriangleCountValue) vertex.value()).idSet();

        if (context.superstep() == 1) {
            int count = 0;

            while (messages.hasNext()) {
                IdList twoDegreeNeighbors = messages.next();
                for (Id twoDegreeNeighbor : twoDegreeNeighbors.values()) {
                    if (allNeighbors.contains(twoDegreeNeighbor)) {
                        count++;
                    }
                }
            }

            return count;
        }
        return null;
    }

    private static Set<Id> getAllNeighbors(Vertex vertex) {
        Set<Id> neighbors = new HashSet<>();
        Edges edges = vertex.edges();
        for (Edge edge : edges) {
            Id targetId = edge.targetId();
            if (!targetId.equals(vertex.id())) {
                neighbors.add(targetId);
            }
        }
        return neighbors;
    }
}
