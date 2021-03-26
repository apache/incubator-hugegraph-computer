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

package com.baidu.hugegraph.computer.core.aggregator;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;

/**
 * Aggregator4Master used by algorithm's master-computation. The master must
 * register all aggregators before all supersteps start. Then the workers can
 * aggregate aggregators in superstep, send the aggregators to master when
 * superstep finish. The master aggregates the aggregators sent by workers.
 * The workers can get the aggregated values master aggregated at next
 * superstep. These values are identical among all workers.
 */
public interface Aggregator4Master {

    /**
     * Register the aggregator with specified name. The name must be unique.
     * Used by algorithm's master-computation to register aggregators.
     */
    <V extends Value> void registerAggregator(
                           String name,
                           Class<? extends Aggregator<V>> aggregatorClass);

    /**
     * Register aggregator with specified value type and a combiner which can
     * combine values with specified value type. The name must be unique.
     * Used by algorithm's master-computation to register aggregators.
     */
    <V extends Value> void registerAggregator(
                           String name,
                           ValueType type,
                           Class<? extends Combiner<V>> combinerClass);

    /**
     * Set the aggregated value by master-computation. The value will be
     * received by workers at next superstep.
     * Throws ComputerException if master does not register the aggregator
     * with specified name.
     */
    <V extends Value> void aggregatedValue(String name, V value);

    /**
     * Get the aggregated value. The aggregated value is aggregated from
     * workers at this superstep.
     * Throws ComputerException if master does not register the aggregator
     * with specified name.
     */
    <V extends Value> V aggregatedValue(String name);
}
