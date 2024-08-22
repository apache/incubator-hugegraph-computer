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

package org.apache.hugegraph.computer.algorithm.path.rings;

import java.util.Iterator;

import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;

public class RingsDetection implements Computation<IdList> {

    @Override
    public String name() {
        return "rings";
    }

    @Override
    public String category() {
        return "path";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new IdListList());
        if (vertex.edges().size() == 0) {
            return;
        }

        // Init path
        Id id = vertex.id();
        IdList path = new IdList();
        path.add(id);

        for (Edge edge : vertex.edges()) {
            /*
             * Only send path to vertex whose id is larger than
             * or equals current vertex id
             */
            if (id.compareTo(edge.targetId()) <= 0) {
                context.sendMessage(edge.targetId(), path);
            }
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<IdList> messages) {
        Id id = vertex.id();
        boolean halt = true;
        while (messages.hasNext()) {
            halt = false;
            IdList sequence = messages.next();
            if (id.equals(sequence.getFirst())) {
                // Use the smallest vertex record ring
                boolean isMin = true;
                for (int i = 1; i < sequence.size(); i++) {
                    Id pathVertexValue = sequence.get(i);
                    if (id.compareTo(pathVertexValue) > 0) {
                        isMin = false;
                        break;
                    }
                }
                if (isMin) {
                    sequence.add(id);
                    IdListList sequences = vertex.value();
                    sequences.add(sequence.copy());
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
                Id ringId = sequence.getFirst();
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
