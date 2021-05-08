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

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.rpc.AggregateRpcService;
import com.baidu.hugegraph.util.E;

/**
 * Aggregator manager manages aggregators in master.
 */
public class MasterAggrManager implements Manager {

    public static final String NAME = "master_aggr";

    private final MasterAggregateHandler handler;

    public MasterAggrManager() {
        this.handler = new MasterAggregateHandler();
    }

    @Override
    public String name() {
        return NAME;
    }

    public AggregateRpcService handler() {
        return this.handler;
    }

    public <V extends Value<?>> void addAggregator(String name,
                                                   Aggregator<V> aggr) {
        this.handler.setAggregator(name, aggr);
    }

    public void clearAggregators() {
        this.handler.clearAggregators();
    }

    private static class MasterAggregateHandler implements AggregateRpcService {

        private final Map<String, Aggregator<Value<?>>> aggregators;

        public MasterAggregateHandler() {
            this.aggregators = new ConcurrentHashMap<>();
        }

        @Override
        public Map<String, Aggregator<Value<?>>> listAggregators() {
            return Collections.unmodifiableMap(this.aggregators);
        }

        @Override
        public void aggregateAggregators(Map<String, Value<?>> aggregators) {
            for (Entry<String, Value<?>> aggr : aggregators.entrySet()) {
                this.aggregateAggregator(aggr.getKey(), aggr.getValue());
            }
        }

        @Override
        public <V extends Value<?>> Aggregator<V> getAggregator(String name) {
            Aggregator<Value<?>> aggregator = this.aggregators.get(name);
            E.checkArgument(aggregator != null,
                            "Not found aggregator '%s'", name);
            @SuppressWarnings("unchecked")
            Aggregator<V> result = (Aggregator<V>) aggregator;
            return result;
        }

        @Override
        public <V extends Value<?>> void aggregateAggregator(String name,
                                                             V value) {
            Aggregator<V> aggregator = this.getAggregator(name);
            aggregator.aggregateValue(value);
        }

        public <V extends Value<?>> void setAggregator(String name,
                                                       Aggregator<V> aggr) {
            @SuppressWarnings("unchecked")
            Aggregator<Value<?>> aggregator = (Aggregator<Value<?>>) aggr;
            this.aggregators.put(name, aggregator);
        }

        public void clearAggregators() {
            this.aggregators.clear();
        }
    }
}
