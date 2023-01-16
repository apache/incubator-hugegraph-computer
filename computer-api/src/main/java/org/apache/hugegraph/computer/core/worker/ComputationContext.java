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

package org.apache.hugegraph.computer.core.worker;

import java.util.Iterator;
import java.util.function.BiFunction;

import org.apache.hugegraph.computer.core.combiner.Combiner;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;

/**
 * The ComputationContext is the interface for the algorithm's vertex
 * computation. It is passed to algorithm's vertex computation as parameter
 * when compute a vertex. It's used by algorithm's vertex computation to send
 * messages and get graph information such as total vertex count and total
 * edge count.
 */
public interface ComputationContext {

    /**
     * Send value to specified target vertex. The specified target vertex
     * will receive the value at next superstep.
     */
    void sendMessage(Id target, Value value);

    /**
     * Send value to all adjacent vertices of specified vertex. The adjacent
     * vertices of specified vertex will receive the value at next superstep.
     */
    default void sendMessageToAllEdges(Vertex vertex, Value value) {
        Iterator<Edge> edges = vertex.edges().iterator();
        while (edges.hasNext()) {
            Edge edge = edges.next();
            this.sendMessage(edge.targetId(), value);
        }
    }

    /**
     * Send all values to filtered adjacent vertices of specified vertex. The
     * filtered adjacent vertices of specified vertex will receive the value
     * at next superstep.
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
     * Send all values to all adjacent vertices of specified vertex. The
     * adjacent vertices of specified vertex will receive the values at next
     * superstep.
     */
    default <M extends Value> void sendMessagesToAllEdges(
                                   Vertex vertex,
                                   Iterator<M> values) {
        while (values.hasNext()) {
            M value = values.next();
            this.sendMessageToAllEdges(vertex, value);
        }
    }

    /**
     * Send all values to filtered adjacent vertices of specified vertex. The
     * filtered adjacent vertices of specified vertex will receive the values
     * at next superstep.
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
     * superstep to superstep, because the algorithm may add or delete vertices
     * during superstep.
     */
    long totalVertexCount();

    /**
     * @return the total edge count of the graph. The value may vary from
     * superstep to superstep, because the algorithm may add or delete edges
     * during superstep.
     */
    long totalEdgeCount();

    /**
     * @return the current superstep.
     */
    int superstep();

    /**
     * @return The message combiner, or null if there is no message combiner.
     * The combiner is used to combine messages for a vertex.
     * For {@link ReduceComputation}, there must be a combiner.
     */
    <V extends Value> Combiner<V> combiner();
}
