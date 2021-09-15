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

package com.baidu.hugegraph.computer.algorithm.path.rings.pathlist;

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;

public class RingsDetection implements Computation<IdListList> {

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

        IdListList message = new IdListList();
        message.add(path);
        for (Edge edge : vertex.edges()) {
            Id targetId = edge.targetId();
            /*
             * Only send path to vertex whose id is larger than
             * or equals current vertex id
             */
            if (id.compareTo(targetId) <= 0) {
                context.sendMessage(edge.targetId(), message);
            }
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<IdListList> messages) {
        Id id = vertex.id();
        boolean halt = true;
        IdListList paths = new IdListList();
        while (messages.hasNext()) {
            halt = false;
            IdListList message = messages.next();
            for (int i = 0; i < message.size(); i++) {
                IdList sequence = message.get(i);
                if (id.equals(sequence.get(0))) {
                    // Use the smallest vertex record ring
                    boolean isMin = true;
                    for (int j = 0; j < sequence.size(); j++) {
                        Id pathVertexValue = sequence.get(j);
                        if (id.compareTo(pathVertexValue) > 0) {
                            isMin = false;
                            break;
                        }
                    }
                    if (isMin) {
                        sequence.add(id);
                        IdListList value = vertex.value();
                        value.add(sequence);
                    }
                    continue;
                } else {
                    boolean contains = false;
                    // Drop sequence if path contains this vertex
                    for (int j = 0; j < sequence.size(); j++) {
                        Id pathVertex = sequence.get(j);
                        if (pathVertex.equals(vertex.id())) {
                            contains = true;
                            break;
                        }
                    }
                    if (contains) {
                        continue;
                    }
                }
                sequence.add(id);
                paths.add(sequence);
            }
        }

        if (paths.size() != 0) {
            for (Edge edge : vertex.edges()) {
                IdListList message = new IdListList();
                Id targetId = edge.targetId();
                for (int i = 0; i < paths.size(); i++) {
                    IdList sequence = paths.get(i);
                    if (sequence.get(0).compareTo(targetId) <= 0) {
                        message.add(sequence);
                    }
                }
                if (message.size() > 0) {
                    context.sendMessage(targetId, message);
                }
            }
        }

        if (halt) {
            vertex.inactivate();
        }
    }
}
