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


import java.util.Iterator;

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
            IdList neighbors = new IdList();

            // Collect outgoing neighbors
            Edges edges = vertex.edges();
            for (Edge edge : edges) {
                Id outId = edge.targetId();
                int compareResult = outId.compareTo(vertex.id());
                if (compareResult != 0 && !allNeighbors.contains(outId)) {
                    // Collect id of id less than self from outgoing neighbors
                    if (compareResult < 0) {
                        neighbors.add(outId);
                    }
                    allNeighbors.add(outId);
                }
            }

            // Collect incoming neighbors
            while (messages.hasNext()) {
                IdList idList = messages.next();
                assert idList.size() == 1;
                Id inId = idList.get(0);
                int compareResult = inId.compareTo(vertex.id());
                if (compareResult != 0 && !allNeighbors.contains(inId)) {
                    // Collect id of id less than self from incoming neighbors
                    if (compareResult < 0) {
                        neighbors.add(inId);
                    }
                    allNeighbors.add(inId);
                }
            }

            // Send all neighbors of id less than self to neighbors
            for (Id targetId : allNeighbors.values()) {
                context.sendMessage(targetId, neighbors);
            }
        } else if (context.superstep() == 2) {
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
