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

package org.apache.hugegraph.computer.algorithm.community.trianglecount;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdSet;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;

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
        IdSet selfId = new IdSet();
        selfId.add(vertex.id());

        context.sendMessageToAllEdgesIf(vertex, selfId, (ids, targetId) -> {
            return !vertex.id().equals(targetId);
        });
        vertex.value(new TriangleCountValue());
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<IdList> messages) {
        IdSet neighbors = ((TriangleCountValue) vertex.value()).idSet();
        Integer count = this.triangleCount(context, vertex, messages, neighbors);
        if (count != null) {
            ((TriangleCountValue) vertex.value()).count(count);
            vertex.inactivate();
        }
    }

    protected Integer triangleCount(ComputationContext context, Vertex vertex,
                                    Iterator<IdList> messages, IdSet neighbors) {
        if (context.superstep() == 1) {
            // Collect outgoing neighbors
            Set<Id> outNeighbors = getOutNeighbors(vertex);
            neighbors.addAll(outNeighbors);

            // Collect incoming neighbors
            while (messages.hasNext()) {
                IdList idList = messages.next();
                assert idList.size() == 1;
                Id inId = idList.getFirst();
                if (!outNeighbors.contains(inId)) {
                    neighbors.add(inId);
                }
            }

            // Send all neighbors to neighbors
            for (Id targetId : neighbors.value()) {
                context.sendMessage(targetId, neighbors);
            }
        } else if (context.superstep() == 2) {
            int count = 0;

            while (messages.hasNext()) {
                IdList twoDegreeNeighbors = messages.next();
                for (Id twoDegreeNeighbor : twoDegreeNeighbors.values()) {
                    if (neighbors.contains(twoDegreeNeighbor)) {
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
