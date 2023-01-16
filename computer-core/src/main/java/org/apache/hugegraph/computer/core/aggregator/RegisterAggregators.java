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

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.util.E;

public class RegisterAggregators {

    private final Map<String, Aggregator<? extends Value>> aggregators;

    public RegisterAggregators() {
        this.aggregators = new ConcurrentHashMap<>();
    }

    public void put(String name, Aggregator<? extends Value> aggregator) {
        E.checkArgument(name != null,
                        "The registered aggregator name can't be null");
        E.checkArgument(!name.isEmpty(),
                        "The registered aggregator name can't be empty");
        E.checkArgument(aggregator != null,
                        "The registered aggregator can't be null");
        this.aggregators.put(name, aggregator);
    }

    public Aggregator<? extends Value> copy(String name) {
        Aggregator<? extends Value> aggregator = this.aggregators.get(name);
        E.checkArgument(aggregator != null,
                        "Can't get unregistered aggregator with name '%s'",
                        name);
        return aggregator.copy();
    }

    public Map<String, Aggregator<? extends Value>> copyAll() {
        Map<String, Aggregator<? extends Value>> copy =
                                                 new ConcurrentHashMap<>();
        for (Entry<String, Aggregator<? extends Value>> aggr :
             this.aggregators.entrySet()) {
            copy.put(aggr.getKey(), aggr.getValue().copy());
        }
        return copy;
    }

    public void clear() {
        this.aggregators.clear();
    }

    public void repair(ComputerContext context) {
        for (Aggregator<? extends Value> aggr : this.aggregators.values()) {
            aggr.repair(context);
        }
    }
}
