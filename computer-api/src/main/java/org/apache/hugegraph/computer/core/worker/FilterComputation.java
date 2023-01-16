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

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.iterator.MapperIterator;

/**
 * FilterComputation is computation that can't combine the messages. When
 * messages are received, it can be computed and decided whether to propagate
 * along the adjacent vertices. FilterComputation suit for computation like
 * ring detection.
 * @param <M> Message type
 */
public interface FilterComputation<M extends Value> extends Computation<M> {

    /**
     * Set vertex's value and return initial message. The message will be
     * used to compute the vertex as parameter Iterator<M> messages.
     * Be called for every vertex in superstep0.
     * {@link #compute0(ComputationContext, Vertex)}.
     */
    M initialValue(ComputationContext context, Vertex vertex);

    /**
     * Compute with initial message. Be called for every vertex in superstep0.
     */
    @Override
    default void compute0(ComputationContext context, Vertex vertex) {
        M result = this.initialValue(context, vertex);
        this.compute(context, vertex, Arrays.asList(result).iterator());
    }

    /**
     * Compute a vertex with messages.
     * Be called in all supersteps(except superstep0) with messages,
     * or in superstep0 with user defined initial message.
     * Inactive the vertex after compute by default.
     */
    @Override
    default void compute(ComputationContext context,
                         Vertex vertex,
                         Iterator<M> messages) {
        Iterator<M> results = this.computeMessages(context, vertex, messages);
        this.sendMessages(context, vertex, results);
        this.updateState(vertex);
    }

    /**
     * Compute vertex with all the messages received and return the results
     * as an iterator. Be called for every vertex in a superstep.
     * Subclass should override this method if want to compute the vertex when
     * no message received.
     */
    default Iterator<M> computeMessages(ComputationContext context,
                                        Vertex vertex,
                                        Iterator<M> messages) {
        // Streaming iterate messages
        return new MapperIterator<>(messages, message -> {
            // May return null if don't want to propagate
            return this.computeMessage(context, vertex, message);
        });
    }

    /**
     * Compute vertex with a message. Be called for each message of a vertex
     * in a superstep. There may be multiple messages for a vertex, so it may
     * be called multiple times for a vertex in a superstep.
     * @return The value need to propagate along the edges, or null when
     * needn't.
     */
    M computeMessage(ComputationContext context,
                     Vertex vertex,
                     M message);

    /**
     * Subclass should override this method if want to send messages to
     * specified adjacent vertices, send to all adjacent vertices by default.
     */
    default void sendMessages(ComputationContext context,
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
