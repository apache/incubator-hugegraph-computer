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

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.computer.core.rpc.AggregateRpcService;
import org.apache.hugegraph.util.E;

/**
 * Aggregator manager manages aggregators in worker.
 */
public class WorkerAggrManager implements Manager {

    public static final String NAME = "worker_aggr";

    private final ComputerContext context;

    private AggregateRpcService service;

    // Registered aggregators from master
    private RegisterAggregators registerAggregators;
    // Cache the aggregators of the previous superstep
    private Map<String, Value> lastAggregators;
    // Cache the aggregators of the current superstep
    private Aggregators currentAggregators;

    public WorkerAggrManager(ComputerContext context) {
        this.context = context;
        this.service = null;
        this.registerAggregators = new RegisterAggregators();
        this.lastAggregators = new HashMap<>();
        this.currentAggregators = new Aggregators();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        // Called when worker init(), this is called after master inited()
        this.registerAggregators = this.service().registeredAggregators();
        this.registerAggregators.repair(this.context);
    }

    @Override
    public void close(Config config) {
        // Called when worker close()
        this.registerAggregators.clear();
        this.lastAggregators.clear();
        this.currentAggregators.clear();
    }

    @Override
    public void beforeSuperstep(Config config, int superstep) {
        /*
         * Reload aggregators from master
         * The framework guaranteed to call this method before
         * computation.beforeSuperstep()
         */
        this.reloadAggregators();
    }

    @Override
    public void afterSuperstep(Config config, int superstep) {
        /*
         * Send aggregators to master
         * The framework guaranteed to call this method after
         * computation.afterSuperstep()
         */
        this.flushAggregators();
    }

    public void service(AggregateRpcService service) {
        E.checkNotNull(service, "service");
        this.service = service;
    }

    public <V extends Value> Aggregator<V> createAggregator(String name) {
        /*
         * Create aggregator for the current superstep, this method would
         * be called once per superstep for each aggregator, generally called
         * when computation.beforeSuperstep().
         */
        @SuppressWarnings("unchecked")
        Aggregator<V> aggr = (Aggregator<V>)
                             this.registerAggregators.copy(name);
        return aggr;
    }

    public <V extends Value> void aggregateValue(String name, V value) {
        /*
         * Update aggregator for the current superstep,
         * generally called when computation.afterSuperstep().
         */
        E.checkArgument(value != null,
                        "Can't set value to null for aggregator '%s'", name);
        Aggregator<Value> aggr = this.currentAggregators.get(name,
                                                             this.service());
        // May be executed in parallel by multiple threads in a worker
        synchronized (aggr) {
            aggr.aggregateValue(value);
        }
    }

    public <V extends Value> V aggregatedValue(String name) {
        // Get aggregator value from the previous superstep
        @SuppressWarnings("unchecked")
        V value = (V) this.lastAggregators.get(name);
        E.checkArgument(value != null,
                        "Can't find aggregator value with name '%s'", name);
        return value;
    }

    private void flushAggregators() {
        Map<String, Value> aggregators = this.currentAggregators.values();
        this.service().aggregateAggregators(aggregators);
        this.currentAggregators.clear();
    }

    private void reloadAggregators() {
        this.lastAggregators = this.service().listAggregators();
        E.checkNotNull(this.lastAggregators, "lastAggregators");

        this.currentAggregators = new Aggregators(
                                  this.registerAggregators.copyAll());
    }

    private AggregateRpcService service() {
        E.checkArgumentNotNull(this.service, "Not init AggregateRpcService");
        return this.service;
    }
}
