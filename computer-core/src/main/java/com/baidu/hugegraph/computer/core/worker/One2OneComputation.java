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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.computer.core.graph.value.IdValueList;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.iterator.MapperIterator;

public interface One2OneComputation<M extends Value> extends Computation<M> {

    @Override
    default void compute0(WorkerContext context, Vertex vertex) {
        M result = this.initialValue(context, vertex);
        this.compute(context, vertex, Arrays.asList(result).iterator());
    }

    @Override
    default void compute(WorkerContext context,
                         Vertex vertex,
                         Iterator<M> messages) {
        Iterator<M> results = this.computeMessages(context, vertex, messages);
        this.sendMessage(context, vertex, results);
    }

    default Iterator<M> computeMessages(WorkerContext context,
                                        Vertex vertex,
                                        Iterator<M> messages) {
        // Streaming iterate messages
        return new MapperIterator<>(messages, message -> {
            // May return null if don't want to propagate
            return this.computeMessage(context, vertex, message);
        });
    }

    default void sendMessage(WorkerContext context, Vertex vertex,
                             Iterator<M> results) {
        context.sendMessagesToAllEdges(vertex, results);
        vertex.inactivate();
    }

    M computeMessage(WorkerContext context, Vertex vertex, M message);


    M initialValue(WorkerContext context, Vertex vertex);
}
