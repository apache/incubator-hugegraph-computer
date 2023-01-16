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

import org.apache.hugegraph.computer.core.combiner.Combiner;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;

/**
 * For algorithm with combiner that combine all the messages, and the
 * algorithm can only receive a message for a vertex. If no message received,
 * null will be passed. Compute message will return a result that send to
 * all adjacent vertices by default.
 */
public interface ReduceComputation<M extends Value>
       extends Computation<M> {

    /**
     * Set vertex's value and return initial message. The message will be
     * used to compute the vertex as parameter Iterator<M> messages.
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
     * Compute the specified vertex with messages.
     * Be called in all supersteps(except superstep0) with messages,
     * or in superstep0 with user defined initial message.
     * Update the vertex's state after compute.
     */
    @Override
    default void compute(ComputationContext context,
                         Vertex vertex,
                         Iterator<M> messages) {
        Combiner<M> combiner = context.combiner();
        M message = Combiner.combineAll(combiner, messages);
        M result = this.computeMessage(context, vertex, message);
        if (result != null) {
            this.sendMessage(context, vertex, result);
        }
        this.updateState(vertex);
    }

    /**
     * Compute the vertex with combined message, or null if no message received.
     * The returned message will be sent to adjacent vertices.
     * For a vertex, this method can be called only one time in a superstep.
     * @param message Combined message, or null if no message received
     */
    M computeMessage(ComputationContext context, Vertex vertex, M message);

    /**
     * Send result to adjacent vertices of a specified vertex. Send result to
     * all adjacent vertices by default.
     * Be called when the result is not null.
     * The algorithm should override this method if the algorithm doesn't want
     * to send the result to all adjacent vertices.
     */
    default void sendMessage(ComputationContext context,
                             Vertex vertex,
                             M result) {
        context.sendMessageToAllEdges(vertex, result);
    }

    /**
     * Set vertex's state after computation, set inactive by default.
     */
    default void updateState(Vertex vertex) {
        vertex.inactivate();
    }
}
