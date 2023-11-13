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

package org.apache.hugegraph.computer.algorithm.community.cc;

import java.util.Iterator;

import org.apache.hugegraph.computer.algorithm.community.trianglecount.TriangleCount;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdSet;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.ComputationContext;

/**
 * ClusteringCoefficient(CC) algorithm could calculate local & the whole graph:
 * 1. local cc: get triangles & degree for current vertex, calculate them
 * 2. whole cc have 2 ways to get the result: (NOT SUPPORTED NOW)
 * - sum all open & closed triangles in graph, and calculate the result
 * - sum all local cc for each vertex, and use avg as the whole graph result
 * <p>
 * And we have 2 ways to count local cc:
 * 1. if we already saved the triangles in each vertex, we can calculate only
 * in superstep0/compute0 to get the result
 * <p>
 * The formula of local CC is: C(v) = 2T / Dv(Dv - 1)
 * v represents one vertex, T represents the triangles of current vertex,
 * D represents the degree of current vertex
 */
public class ClusteringCoefficient extends TriangleCount {

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
            return !ids.getFirst().equals(targetId);
        });
        vertex.value(new ClusteringCoefficientValue());
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex, Iterator<IdList> messages) {
        IdSet neighbors = ((ClusteringCoefficientValue) vertex.value()).idSet();
        Integer count = super.triangleCount(context, vertex, messages, neighbors);
        if (count != null) {
            ((ClusteringCoefficientValue) vertex.value()).count(count);
            vertex.inactivate();
        }
    }
}
