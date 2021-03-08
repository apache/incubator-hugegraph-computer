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

public interface Computation<M extends Value> {

    /**
     * Called at superstep0, with no messages.
     */
    default void compute0(WorkerContext context, Vertex vertex) {
        M result = this.initialValue(context, vertex);
        this.compute(context, vertex, Arrays.asList(result).iterator());
    }

    /**
     * Set vertex's value and return initial message. The message will be
     * used to compute the vertex as parameter Iterator<M> messages.
     */
    M initialValue(WorkerContext context, Vertex vertex);

    /**
     * Called at all other supersteps except superstep0, with messages.
     */
    void compute(WorkerContext context, Vertex vertex, Iterator<M> messages);

    /**
     * Used to add the resources the computation needed. This method is
     * called only one time. The subclass may optional override this method.
     */
    default void init(WorkerContext context) {
    }

    /**
     * Close the resources used in the computation. This method is called
     * only one time after all superstep iteration.
     */
    default void close(WorkerContext context) {
    }

    /**
     * This method is called before every superstep.
     */
    default void beforeSuperstep(WorkerContext context) {
    }

    /**
     * This method is called after every superstep.
     */
    default void afterSuperstep(WorkerContext context) {
    }
}
