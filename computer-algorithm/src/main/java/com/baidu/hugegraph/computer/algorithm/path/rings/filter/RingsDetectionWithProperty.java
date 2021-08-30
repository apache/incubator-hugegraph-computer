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

package com.baidu.hugegraph.computer.algorithm.path.rings.filter;

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;

public class RingsDetectionWithProperty implements
                                        Computation<RingsDetectionValue> {

    private SpreadFilter filter;

    @Override
    public String name() {
        return "ringsDetectionWithProperty";
    }

    @Override
    public String category() {
        return "path";
    }

    @Override
    public void init(Config config) {
        this.filter = new SpreadFilter(config);
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new IdListList());
        if (vertex.edges().size() == 0 || !this.filter.filter(vertex)) {
            return;
        }

        RingsDetectionValue message = new RingsDetectionValue();
        message.addPath(vertex);
        for (Edge edge : vertex.edges()) {
            if (this.filter.filter(edge)) {
                message.walkEdgeProp(edge.properties());
                context.sendMessage(edge.targetId(), message);
            }
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<RingsDetectionValue> messages) {
        boolean halt = true;
        if (this.filter.filter(vertex)) {
            Id vertexId = vertex.id();
            while (messages.hasNext()) {
                halt = false;
                RingsDetectionValue message = messages.next();
                IdList path = message.path();
                if (vertexId.equals(path.get(0))) {
                    // Use the smallest vertex record ring
                    boolean isMin = true;
                    for (int i = 0; i < path.size(); i++) {
                        Id pathVertexValue = path.get(i);
                        if (vertexId.compareTo(pathVertexValue) > 0) {
                            isMin = false;
                            break;
                        }
                    }
                    if (isMin) {
                        path.add(vertexId);
                        IdListList value = vertex.value();
                        value.add(path.copy());
                    }
                } else {
                    boolean contains = false;
                    // Drop sequence if path contains this vertex
                    for (int i = 0; i < path.size(); i++) {
                        Id pathVertexValue = path.get(i);
                        if (pathVertexValue.equals(vertex.id())) {
                            contains = true;
                            break;
                        }
                    }
                    if (!contains) {
                        path.add(vertex.id());
                        for (Edge edge : vertex.edges()) {
                            if (this.filter.filter(edge, message)) {
                                message.walkEdgeProp(edge.properties());
                                context.sendMessage(edge.targetId(), message);
                            }
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
