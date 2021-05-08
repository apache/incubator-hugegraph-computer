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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.rpc.AggregateRpcService;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;

/**
 * Aggregator manager manages aggregators in worker.
 */
public class WorkerAggrManager implements Manager {

    public static final String NAME = "worker_aggr";

    private AggregateRpcService service;
    private WorkerAggregateCache aggregatorsCache;

    public WorkerAggrManager() {
        this.service = null;
        this.aggregatorsCache = new WorkerAggregateCache(ImmutableMap.of());
    }

    @Override
    public String name() {
        return NAME;
    }

    public void service(AggregateRpcService service) {
        E.checkNotNull(service, "service");
        this.service = service;
    }

    public void flushAggregators() {
        Map<String, Value<?>> aggregators = this.aggregatorsCache
                                                .aggregatorValues();
        this.service().aggregateAggregators(aggregators);
    }

    public void reloadAggregators() {
        Map<String, Aggregator<Value<?>>> aggregators = this.service()
                                                            .listAggregators();
        this.aggregatorsCache = new WorkerAggregateCache(aggregators);
    }

    public <V extends Value<?>> Aggregator<V> aggregator(String name) {
        return this.aggregatorsCache.getAggregator(name, this.service());
    }

    private AggregateRpcService service() {
        E.checkArgumentNotNull(this.service, "Not init AggregateRpcService");
        return this.service;
    }

    private static class WorkerAggregateCache {

        private final Map<String, Aggregator<Value<?>>> aggregators;

        public WorkerAggregateCache(Map<String, Aggregator<Value<?>>> aggrs) {
            this.aggregators = new ConcurrentHashMap<>(aggrs);
        }

        public Map<String, Value<?>> aggregatorValues() {
            Map<String, Value<?>> values = new HashMap<>();
            for (Entry<String, Aggregator<Value<?>>> aggr :
                 this.aggregators.entrySet()) {
                values.put(aggr.getKey(), aggr.getValue().aggregatedValue());
            }
            return values;
        }

        public <V extends Value<?>> Aggregator<V> getAggregator(
                                                  String name,
                                                  AggregateRpcService service) {
            Aggregator<Value<?>> aggregator = this.aggregators.get(name);
            if (aggregator == null) {
                aggregator = service.getAggregator(name);
                E.checkArgument(aggregator != null,
                                "Can't get aggregator '%s'", name);
                this.aggregators.put(name, aggregator);
            }
            @SuppressWarnings("unchecked")
            Aggregator<V> result = (Aggregator<V>) aggregator;
            return result;
        }
    }
}
