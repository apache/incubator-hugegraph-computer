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

package org.apache.hugegraph.computer.core.aggregator;

import org.apache.hugegraph.computer.core.graph.value.Value;

/**
 * Aggregator4Worker used by algorithm's computation. The computation can
 * only use the aggregator with specified name the master registered.
 * See {@link Aggregator4Master} for detailed information about aggregation
 * process.
 */
public interface Aggregator4Worker {

    /**
     * Create aggregator by name in worker-computation, the aggregator is
     * registered by master-computation.
     * Used by algorithm's worker-computation beforeSuperstep(), can be called
     * in each superstep.
     * Throws ComputerException if master-computation does not register
     * aggregator with the specified name.
     */
    <V extends Value> Aggregator<V> createAggregator(String name);

    /**
     * Set aggregate value in worker. The value of aggregator will be
     * aggregated locally in worker first, and it would be sent to master when
     * the current superstep finish.
     * Used by algorithm's worker-computation afterSuperstep(), can be called
     * in each superstep.
     * Throws ComputerException if master-computation does not register
     * aggregator with the specified name.
     * @param value The value to be aggregated
     */
    <V extends Value> void aggregateValue(String name, V value);

    /**
     * Get the aggregated value in worker-computation, the value is aggregated
     * by master at previous superstep, it won't be changed in a superstep.
     * Each worker aggregate an aggregator value locally, then submit to master,
     * then master aggregate the aggregator values from all workers.
     * master-computation can get the aggregated value in master compute(), and
     * worker-computation can get the aggregated value in the next superstep.
     * Used by algorithm's worker-computation compute(), can be called in
     * each superstep.
     * Throws ComputerException if master-computation does not register
     * aggregator with the specified name.
     */
    <V extends Value> V aggregatedValue(String name);
}
