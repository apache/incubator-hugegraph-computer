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

package com.baidu.hugegraph.computer.algorithm.rings;

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;

public class RingsDetection implements Computation<IdList> {

    @Override
    public String name() {
        return "ringsDetection";
    }

    @Override
    public String category() {
        return "rings";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        if (vertex.edges().size() == 0) {
            return;
        }
        vertex.value(new IdListList());

        Id vertexId = vertex.id();

        IdList path = new IdList();
        path.add(vertexId);

        for (Edge edge : vertex.edges()) {
            /*
             * Only send path to vertex whose id is larger than
             * or equals current vertex id
             */
            if (vertexId.compareTo(edge.targetId()) <= 0) {
                context.sendMessage(edge.targetId(), path);
            }
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<IdList> messages) {
        Id vertexId = vertex.id();
        boolean halt = true;
        while (messages.hasNext()) {
            halt = false;
            IdList sequence = messages.next();
            if (vertexId.equals(sequence.get(0))) {
                // Use the smallest vertex record ring
                boolean isMin = true;
                for (int i = 0; i < sequence.size(); i++) {
                    Id pathVertexValue = sequence.get(i);
                    if (vertexId.compareTo(pathVertexValue) > 0) {
                        isMin = false;
                        break;
                    }
                }
                if (isMin) {
                    sequence.add(vertexId);
                    IdListList value = vertex.value();
                    value.add(sequence.copy());
                }
            } else {
                boolean contains = false;
                // Drop sequence if path contains this vertex
                for (int i = 0; i < sequence.size(); i++) {
                    Id pathVertexValue = sequence.get(i);
                    if (pathVertexValue.equals(vertex.id())) {
                        contains = true;
                        break;
                    }
                }
                // Field ringId is smallest vertex id in path
                Id ringId = sequence.get(0);
                if (!contains) {
                    sequence.add(vertex.id());
                    for (Edge edge : vertex.edges()) {
                        if (ringId.compareTo(edge.targetId()) <= 0) {
                            context.sendMessage(edge.targetId(), sequence);
                        }
                    }
                }
            }
        }
        if (halt) {
            vertex.inactivate();
        }
    }
}
