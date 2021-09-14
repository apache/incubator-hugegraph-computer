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
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;

public class TriangleCount implements Computation<IdList> {

    @Override
    public String name() {
        return "triangle_count";
    }

    @Override
    public String category() {
        return "community";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        IdList selfId = new IdList();
        selfId.add(vertex.id());

        context.sendMessageToAllEdgesIf(vertex, selfId, (ids, targetId) -> {
            return !ids.get(0).equals(targetId);
        });
        vertex.value(new TriangleCountValue());
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<IdList> messages) {
        Long count = this.triangleCount(context, vertex, messages);
        if (count != null) {
            ((TriangleCountValue) vertex.value()).count(count);
            vertex.inactivate();
        }
    }

    private Long triangleCount(ComputationContext context, Vertex vertex,
                               Iterator<IdList> messages) {
        IdList neighbors = ((TriangleCountValue) vertex.value()).idList();

        if (context.superstep() == 1) {
            // Collect outgoing neighbors
            Set<Id> outNeighbors = getOutNeighbors(vertex, neighbors);

            // Collect incoming neighbors
            while (messages.hasNext()) {
                IdList idList = messages.next();
                assert idList.size() == 1;
                Id inId = idList.get(0);
                if (!outNeighbors.contains(inId)) {
                    neighbors.add(inId);
                }
            }

            // Send all neighbors to neighbors
            for (Id targetId : neighbors.values()) {
                context.sendMessage(targetId, neighbors);
            }
        } else if (context.superstep() == 2) {
            long count = 0L;

            Set<Id> allNeighbors = new HashSet<>(neighbors.values());
            while (messages.hasNext()) {
                IdList idList = messages.next();
                for (Id value : idList.values()) {
                    if (allNeighbors.contains(value)) {
                        count++;
                    }
                }
            }

            return count >> 1;
        }
        return null;
    }

    private static Set<Id> getOutNeighbors(Vertex vertex, IdList neighbors) {
        Set<Id> outNeighbors = new HashSet<>();
        Edges edges = vertex.edges();
        for (Edge edge : edges) {
            Id targetId = edge.targetId();
            if (!vertex.id().equals(targetId)) {
                boolean added = outNeighbors.add(targetId);
                if (added) {
                    neighbors.add(targetId);
                }
            }
        }
        return outNeighbors;
    }
}
