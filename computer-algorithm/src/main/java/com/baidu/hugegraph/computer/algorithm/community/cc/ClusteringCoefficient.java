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


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
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
        IdList selfId = new IdList();
        selfId.add(vertex.id());

        context.sendMessageToAllEdgesIf(vertex, selfId, (ids, targetId) -> {
            return !ids.get(0).equals(targetId);
        });
        vertex.value(new ClusteringCoefficientValue());
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<IdList> messages) {
        Long count = this.triangleCount(context, vertex, messages);
        if (count != null) {
            ((ClusteringCoefficientValue) vertex.value()).count(count);
            vertex.inactivate();
        }
    }

    private Long triangleCount(ComputationContext context, Vertex vertex,
                               Iterator<IdList> messages) {
        IdList neighbors = ((ClusteringCoefficientValue) vertex
                                                         .value()).idList();

        if (context.superstep() == 1) {
            // Collect outgoing neighbors
            Set<Id> outNeighbors = getOutNeighbors(vertex);
            neighbors.addAll(outNeighbors);

            // Collect incoming neighbors
            while (messages.hasNext()) {
                IdList idList = messages.next();
                assert idList.size() == 1;
                Id inId = idList.get(0);
                if (!outNeighbors.contains(inId)) {
                    neighbors.add(inId);
                }
            }
            // Save degree to vertex value here (optional)
            ((ClusteringCoefficientValue) vertex.value())
                                          .setDegree(neighbors.size());

            // Send all neighbors to neighbors
            for (Id targetId : neighbors.values()) {
                context.sendMessage(targetId, neighbors);
            }
        } else if (context.superstep() == 2) {
            long count = 0L;

            Set<Id> allNeighbors = new HashSet<>(neighbors.values());
            while (messages.hasNext()) {
                IdList twoDegreeNeighbors = messages.next();
                for (Id twoDegreeNeighbor : twoDegreeNeighbors.values()) {
                    if (allNeighbors.contains(twoDegreeNeighbor)) {
                        count++;
                    }
                }
            }

            return count >> 1;
        }
        return null;
    }

    private static Set<Id> getOutNeighbors(Vertex vertex) {
        Set<Id> outNeighbors = new HashSet<>();
        Edges edges = vertex.edges();
        for (Edge edge : edges) {
            Id targetId = edge.targetId();
            if (!vertex.id().equals(targetId)) {
                outNeighbors.add(targetId);
            }
        }
        return outNeighbors;
    }
}
