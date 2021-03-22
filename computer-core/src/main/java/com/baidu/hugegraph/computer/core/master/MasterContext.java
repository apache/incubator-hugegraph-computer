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

package com.baidu.hugegraph.computer.core.master;

import com.baidu.hugegraph.computer.core.aggregator.Aggregator;
import com.baidu.hugegraph.computer.core.graph.value.Value;

/**
 * The MasterContext is the interface from the algorithm's master computation.
 * Other master's internal services are defined in MasterService.
 */
public interface MasterContext {

    /**
     * Register the aggregator with specified name.
     * TODO: try pass aggregator instance
     */
    <V extends Value> void registerAggregator(
                      String name,
                      Class<? extends Aggregator<V>> aggregatorClass);

    /**
     * Get the aggregated value. The aggregated value is sent from workers at
     * previous superstep.
     */
    <V extends Value> V aggregatedValue(String name);

    /**
     * Set the aggregated value. The value is received by workers at this
     * superstep.
     */
    <V extends Value> void aggregatedValue(String name, V value);

    /**
     * @return the total vertex count of the graph. The value may vary from
     * superstep to superstep.
     */
    long totalVertexCount();

    /**
     * @return the total edge count of the graph. The value may vary from
     * superstep to superstep.
     */
    long totalEdgeCount();

    /**
     * @return the vertex count that is inactive.
     */
    long finishedVertexCount();

    /**
     * @return the message count at previous superstep.
     */
    long messageCount();

    /**
     * @return the message received in bytes at previous superstep.
     */
    long messageBytes();

    /**
     * @return the current superstep.
     */
    int superstep();
}
