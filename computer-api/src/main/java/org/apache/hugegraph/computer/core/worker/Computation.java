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

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;

/**
 * Computation is used to compute vertex. It is the most important algorithm
 * interface. #compute() is called for every active vertex in a superstep.
 * The algorithm can set the vertex inactive. If a vertex is inactive and no
 * message received, the vertex will not be computed. If a vertex is inactive
 * and any message is received, the framework will reactivate the vertex and the
 * vertex will be computed. The algorithm can inactivate a vertex many times.
 * If a vertex is active(not set inactive by algorithm), the vertex will be
 * computed.
 * @param <M>
 */
public interface Computation<M extends Value> {

    /**
     * @return the name of computation.
     */
    String name();

    /**
     * @return category of the computation.
     */
    String category();

    /**
     * Compute a vertex without message in superstep0. Every vertex is active in
     * superstep0. It should set vertex's initial value in this method. Be
     * called for every vertex in superstep0.
     */
    void compute0(ComputationContext context, Vertex vertex);

    /**
     * Compute a active vertex. If the vertex received messages, pass
     * messages by an iterator. If the vertex does not received any message, an
     * empty Iterator will be passed.
     * Be called for every vertex in all supersteps(except superstep0).
     */
    void compute(ComputationContext context,
                 Vertex vertex,
                 Iterator<M> messages);

    /**
     * This method is called only one time before all superstep start.
     * Subclass can override this method if want to init the resources the
     * computation needed, like create a connection to other database, get
     * the config the algorithm used.
     */
    default void init(Config config) {
        // pass
    }

    /**
     * This method is called only one time after all superstep iteration.
     * Subclass can override this method if want to close the resources used
     * in the computation.
     */
    default void close(Config config) {
        // pass
    }

    /**
     * This method is called before every superstep. Subclass can override
     * this method if want to get aggregators master aggregated at previous
     * superstep.
     */
    default void beforeSuperstep(WorkerContext context) {
        // pass
    }

    /**
     * This method is called after every superstep. Subclass can override
     * this method if want to aggregate the aggregators to master.
     */
    default void afterSuperstep(WorkerContext context) {
        // pass
    }
}
