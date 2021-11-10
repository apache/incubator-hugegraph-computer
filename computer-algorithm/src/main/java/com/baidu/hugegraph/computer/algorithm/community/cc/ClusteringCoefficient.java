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

package com.baidu.hugegraph.computer.algorithm.community.cc;


import java.util.Iterator;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdSet;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;

/**
 * ClusteringCoefficient(CC) algorithm could calculate local & the whole graph:
 * 1. local cc: get triangles & degree for current vertex, calculate them
 * 2. whole cc have 2 ways to get the result: (NOT SUPPORTED NOW)
 *    - sum all open & closed triangles in graph, and calculate the result
 *    - sum all local cc for each vertex, and use avg as the whole graph result
 *
 * And we have 2 ways to count local cc:
 * 1. if we already saved the triangles in each vertex, we can calculate only
 *    in superstep0/compute0 to get the result
 * 2. if we want recount the triangles result, we can choose:
 *    - copy code from TriangleCount, then add extra logic
 *    - reuse code in TriangleCount (need solve compatible problem - TODO)
 *
 *  The formula of local CC is: C(v) = 2T / Dv(Dv - 1)
 *  v represents one vertex, T represents the triangles of current vertex,
 *  D represents the degree of current vertex
 */
public class ClusteringCoefficient implements Computation<IdList> {

    @Override
    public String name() {
        return "clustering_coefficient";
    }

    @Override
    public String category() {
        return "community";
    }

    @Override
    public void init(Config config) {
        // Reuse triangle count later
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        ClusteringCoefficientValue value = new ClusteringCoefficientValue();
        IdSet allNeighbors = value.idSet();

        IdList neighbors = new IdList();
        for (Edge edge : vertex.edges()) {
            Id targetId = edge.targetId();
            int compareResult = targetId.compareTo(vertex.id());
            if (compareResult != 0) {
                // Collect neighbors of id less than self from all neighbors
                if (compareResult < 0 && !allNeighbors.contains(targetId)) {
                    neighbors.add(targetId);
                }
                // Collect all neighbors, include of incoming and outgoing
                allNeighbors.add(targetId);
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
            ((ClusteringCoefficientValue) vertex.value()).count(count);
            vertex.inactivate();
        }
    }

    private Integer triangleCount(ComputationContext context, Vertex vertex,
                                  Iterator<IdList> messages) {
        IdSet allNeighbors = ((ClusteringCoefficientValue) vertex.value())
                                                                 .idSet();

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
}
