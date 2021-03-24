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

import java.util.Arrays;
import java.util.Iterator;

import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.iterator.MapperIterator;

/**
 * FilterMapComputation is computation that can't combine the messages. When
 * messages are received, it can be computed and decided whether to propagate
 * along the adjacent vertices. FilterMapComputation suit for computation like
 * ring detection.
 * @param <M> Message type
 */
public interface FilterMapComputation<M extends Value> extends Computation<M> {

    /**
     * Set vertex's value and return initial message. The message will be
     * used to compute the vertex as parameter Iterator<M> messages.
     * The method is invoked at superstep0,
     * {@link #compute0(VertexComputationContext, Vertex)}.
     */
    M initialValue(VertexComputationContext context, Vertex vertex);

    /**
     * Compute with initial message. Be invoked at superstep0 for every vertex.
     */
    @Override
    default void compute0(VertexComputationContext context, Vertex vertex) {
        M result = this.initialValue(context, vertex);
        this.compute(context, vertex, Arrays.asList(result).iterator());
    }

    /**
     * Compute a vertex with messages.
     * Called at all supersteps(except superstep0) with messages,
     * or at superstep0 with user defined initial message.
     * Subclass should override this method if want to compute the vertex when
     * no messages received.
     * Inactive the vertex after compute by default.
     */
    @Override
    default void compute(VertexComputationContext context,
                         Vertex vertex,
                         Iterator<M> messages) {
        Iterator<M> results = this.computeMessages(context, vertex, messages);
        this.sendMessages(context, vertex, results);
        this.updateState(vertex);
    }

    /**
     * Compute vertex with all the messages received and get the results as
     * iterator. Be invoked by
     * {@link #compute(VertexComputationContext, Vertex, Iterator)}.
     */
    default Iterator<M> computeMessages(VertexComputationContext context,
                                        Vertex vertex,
                                        Iterator<M> messages) {
        // Streaming iterate messages
        return new MapperIterator<>(messages, message -> {
            // May return null if don't want to propagate
            return this.computeMessage(context, vertex, message);
        });
    }

    /**
     * Compute vertex with a message. This method will be called once for each
     * messages of a vertex in a superstep.
     * @return The value need to propagate along the edges, or null when
     * needn't.
     */
    M computeMessage(VertexComputationContext context,
                     Vertex vertex,
                     M message);

    /**
     * Subclass should override this method if want to send messages to
     * specified adjacent vertices, send to all adjacent vertices by default.
     */
    default void sendMessages(VertexComputationContext context,
                              Vertex vertex,
                              Iterator<M> results) {
        context.sendMessagesToAllEdges(vertex, results);
    }

    /**
     * Set vertex's state after computation, set inactive by default.
     */
    default void updateState(Vertex vertex) {
        vertex.inactivate();
    }
}
