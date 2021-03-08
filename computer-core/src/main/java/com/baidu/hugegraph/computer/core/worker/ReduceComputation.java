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

public interface ReduceComputation<M extends Value> extends Computation<M> {

    @Override
    default void compute(WorkerContext context, Vertex vertex,
                         Iterator<M> messages) {
        M message = Combiner.combineAll(context.combiner(), messages);
        M result = this.computeMessage(context, vertex, message);
        this.sendMessage(context, vertex, result);
    }

    default void sendMessage(WorkerContext context, Vertex vertex, M result) {
        context.sendMessageToAllEdges(vertex, result);
    }

    /**
     * TODO: add description
     * @param message Null if no message received
     */
    M computeMessage(WorkerContext context, Vertex vertex, M message);
}
