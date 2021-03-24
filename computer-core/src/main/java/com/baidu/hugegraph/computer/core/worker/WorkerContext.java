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

package com.baidu.hugegraph.computer.core.worker;

import java.util.Iterator;
import java.util.function.BiFunction;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

/**
 * The WorkerContext is the interface for the algorithm's vertex computation.
 * Other worker's internal services are defined in WorkerService.
 */
public interface WorkerContext {

    /**
     * Get config
     */
    Config config();

    /**
     * Send value to specified target vertex.
     */
    void sendMessage(Id target, Value value);

    /**
     * Send value to all edges of vertex.
     */
    default void sendMessageToAllEdges(Vertex vertex, Value value) {
        Iterator<Edge> edges = vertex.edges().iterator();
        while (edges.hasNext()) {
            Edge edge = edges.next();
            this.sendMessage(edge.targetId(), value);
        }
    }

    /**
     * Send all values to all filtered edges of vertex
     */
    default <M extends Value> void sendMessageToAllEdgesIf(
                                   Vertex vertex,
                                   M value,
                                   BiFunction<M, Id, Boolean> filter) {
        Iterator<Edge> edges = vertex.edges().iterator();
        while (edges.hasNext()) {
            Edge edge = edges.next();
            if (filter.apply(value, edge.targetId())) {
                this.sendMessage(edge.targetId(), value);
            }
        }
    }

    /**
     * Send all values to all edges of vertex
     */
    default <M extends Value> void sendMessagesToAllEdges(Vertex vertex,
                                                          Iterator<M> values) {
        while (values.hasNext()) {
            M value = values.next();
            this.sendMessageToAllEdges(vertex, value);
        }
    }

    /**
     * Send all values to all filtered edges of vertex
     */
    default <M extends Value> void sendMessagesToAllEdgesIf(
                                   Vertex vertex,
                                   Iterator<M> values,
                                   BiFunction<M, Id, Boolean> filter) {
        while (values.hasNext()) {
            M value = values.next();
            Iterator<Edge> edges = vertex.edges().iterator();
            while (edges.hasNext()) {
                Edge edge = edges.next();
                if (filter.apply(value, edge.targetId())) {
                    this.sendMessage(edge.targetId(), value);
                }
            }
        }
    }

    /**
     * @return the total vertex count of the graph. The value may vary from
     * superstep to superstep.
     */
    long totalVertexCount();

    /**
     * @return the total edge count of the graph. The value may vary from
     * superstep to superstep.
     */
    long totalEdgeCount();

    /**
     * @return the current superstep.
     */
    int superstep();

    /**
     * @return The message combiner.
     */
    <V extends Value> Combiner<V> combiner();
}
