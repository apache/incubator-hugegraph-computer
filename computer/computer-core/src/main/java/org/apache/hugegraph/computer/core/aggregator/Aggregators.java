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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.rpc.AggregateRpcService;
import org.apache.hugegraph.util.E;

import com.google.common.collect.ImmutableMap;

public class Aggregators {

    private final Map<String, Aggregator<? extends Value>> aggregators;

    public Aggregators() {
        this(ImmutableMap.of());
    }

    public Aggregators(Map<String, Aggregator<? extends Value>> aggrs) {
        this.aggregators = new ConcurrentHashMap<>(aggrs);
    }

    public Map<String, Value> values() {
        Map<String, Value> values = new HashMap<>();
        for (Entry<String, Aggregator<? extends Value>> aggr :
             this.aggregators.entrySet()) {
            values.put(aggr.getKey(), aggr.getValue().aggregatedValue());
        }
        return values;
    }

    public <V extends Value> Aggregator<V> get(String name,
                                                  AggregateRpcService service) {
        Aggregator<?> aggregator = this.aggregators.get(name);
        if (aggregator == null) {
            if (service != null) {
                // Try to get the aggregator maybe created dynamic
                aggregator = service.getAggregator(name);
                if (aggregator != null) {
                    this.aggregators.put(name, aggregator);
                }
            }
            E.checkArgument(aggregator != null,
                            "Can't get aggregator '%s'", name);
        }
        @SuppressWarnings("unchecked")
        Aggregator<V> result = (Aggregator<V>) aggregator;
        return result;
    }

    public void reset(RegisterAggregators register) {
        this.aggregators.clear();
        this.aggregators.putAll(register.copyAll());
    }

    public void clear() {
        this.aggregators.clear();
    }
}
