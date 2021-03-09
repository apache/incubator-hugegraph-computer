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

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

/**
 * For algorithm with combiner that combine all the messages, and the
 * algorithm can only receive a message for a vertex. If no message received,
 * null will be passed. Compute message will return a
 * result that propagate along the edges by default.
 */
public interface ReduceComputation<M extends Value> extends Computation<M> {

    @Override
    default void compute(WorkerContext context,
                         Vertex vertex,
                         Iterator<M> messages) {
        M message = Combiner.combineAll(context.combiner(), messages);
        M result = this.computeMessage(context, vertex, message);
        this.sendMessage(context, vertex, result);
    }

    /**
     * By default, computed result will be propagated along the edges.
     * If the algorithm wants to decide not to send the result along all
     * edges, it can override this method.
     */
    default void sendMessage(WorkerContext context, Vertex vertex, M result) {
        context.sendMessageToAllEdges(vertex, result);
    }

    /**
     * For a vertex, this method can be called only one time in a superstep.
     * Compute the vertex with combined message, or null if no message received.
     * The returned message will be passed along the edges.
     * @param message Combined message, or null if no message received
     */
    M computeMessage(WorkerContext context, Vertex vertex, M message);
}
