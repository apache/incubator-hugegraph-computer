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

package org.apache.hugegraph.computer.algorithm.community.wcc;

import java.util.Iterator;

import org.apache.hugegraph.computer.core.combiner.Combiner;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.computer.core.worker.ComputationContext;

/**
 * Wcc stands for Weak Connected Component.
 */
public class Wcc implements Computation<Id> {

    @Override
    public String name() {
        return "wcc";
    }

    @Override
    public String category() {
        return "community";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        Id min = vertex.id();
        for (Edge edge : vertex.edges()) {
            if (edge.targetId().compareTo(min) < 0) {
                min = edge.targetId();
            }
        }
        Id value = min;
        vertex.value(value);
        vertex.inactivate();
        context.sendMessageToAllEdgesIf(vertex, value, (result, target) -> {
            return result.compareTo(target) < 0;
        });
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<Id> messages) {
        Id message = Combiner.combineAll(context.combiner(), messages);
        Id value = vertex.value();
        if (value.compareTo(message) > 0) {
            vertex.value(message);
            context.sendMessageToAllEdges(vertex, message);
        }
        vertex.inactivate();
    }
}
