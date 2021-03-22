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
 * FilterMapComputation suit for computation like ring detection, when a
 * message received, it can be computed and decided whether to propagate
 * along the edges.
 * @param <M> Message type
 */
public interface FilterMapComputation<M extends Value> extends Computation<M> {

    /**
     * Set vertex's value and return initial message. The message will be
     * used to compute the vertex as parameter Iterator<M> messages.
     */
    M initialValue(WorkerContext context, Vertex vertex);

    @Override
    default void compute0(WorkerContext context, Vertex vertex) {
        M result = this.initialValue(context, vertex);
        this.compute(context, vertex, Arrays.asList(result).iterator());
    }

    /**
     * Called at all supersteps(except superstep0) with messages,
     * or at superstep0 with user defined message, in generally the message
     * returned by the user is an empty message.
     */
    @Override
    default void compute(WorkerContext context,
                         Vertex vertex,
                         Iterator<M> messages) {
        Iterator<M> results = this.computeMessages(context, vertex, messages);
        this.sendMessages(context, vertex, results);
        this.updateState(vertex);
    }

    /**
     * Compute all the messages and get the results as iterator.
     */
    default Iterator<M> computeMessages(WorkerContext context,
                                        Vertex vertex,
                                        Iterator<M> messages) {
        // Streaming iterate messages
        return new MapperIterator<>(messages, message -> {
            // May return null if don't want to propagate
            return this.computeMessage(context, vertex, message);
        });
    }

    /**
     * Compute the message. This method will be called once for each message
     * in a superstep.
     * @return The value need to propagate along the edges, or null when
     * needn't.
     */
    M computeMessage(WorkerContext context, Vertex vertex, M message);

    /**
     * Subclass should override this method if want to send messages to
     * specified adjacent vertices, send to all adjacent vertices by default.
     */
    default void sendMessages(WorkerContext context,
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
