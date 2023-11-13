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

package org.apache.hugegraph.computer.algorithm.path.rings.filter;

import java.util.Iterator;

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;

public class RingsDetectionWithFilter
       implements Computation<RingsDetectionMessage> {

    public static final String OPTION_FILTER = "rings.property_filter";

    private SpreadFilter filter;

    @Override
    public String name() {
        return "rings_with_filter";
    }

    @Override
    public String category() {
        return "path";
    }

    @Override
    public void init(Config config) {
        this.filter = new SpreadFilter(config.getString(OPTION_FILTER, "{}"));
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(new IdListList());
        if (vertex.edges().size() == 0 || !this.filter.filter(vertex)) {
            return;
        }

        RingsDetectionMessage message = new RingsDetectionMessage();
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
                        Iterator<RingsDetectionMessage> messages) {
        boolean halt = true;
        if (this.filter.filter(vertex)) {
            Id vertexId = vertex.id();
            while (messages.hasNext()) {
                halt = false;
                RingsDetectionMessage message = messages.next();
                IdList path = message.path();
                if (vertexId.equals(path.getFirst())) {
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
