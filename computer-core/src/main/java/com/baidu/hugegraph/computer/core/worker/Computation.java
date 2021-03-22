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

import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public interface Computation<M extends Value> {

    /**
     * Called at superstep0, with no messages. It should set vertex's initial
     * value in this method.
     */
    void compute0(WorkerContext context, Vertex vertex);

    /**
     * Called at all supersteps(except superstep0) with messages.
     */
    void compute(WorkerContext context, Vertex vertex, Iterator<M> messages);

    /**
     * Used to add the resources the computation needed. This method is
     * called only one time.
     */
    default void init(WorkerContext context) {
        // pass
    }

    /**
     * Close the resources used in the computation. This method is called
     * only one time after all superstep iteration.
     */
    default void close(WorkerContext context) {
        // pass
    }

    /**
     * This method is called before every superstep.
     */
    default void beforeSuperstep(WorkerAggrContext context) {
        // pass
    }

    /**
     * This method is called after every superstep.
     */
    default void afterSuperstep(WorkerAggrContext context) {
        // pass
    }
}
