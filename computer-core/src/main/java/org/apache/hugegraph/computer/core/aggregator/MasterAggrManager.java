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

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.computer.core.rpc.AggregateRpcService;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Aggregator manager manages aggregators in master.
 */
public class MasterAggrManager implements Manager {

    public static final String NAME = "master_aggr";

    private static final Logger LOG = Log.logger(MasterAggrManager.class);

    private final RegisterAggregators registerAggregators;
    private final MasterAggregateHandler aggregatorsHandler;

    public MasterAggrManager() {
        this.registerAggregators = new RegisterAggregators();
        this.aggregatorsHandler = new MasterAggregateHandler();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void inited(Config config) {
        /*
         * Apply aggregators registerd by master init(), so that workers can
         * get aggregators from master.
         */
        this.aggregatorsHandler.resetAggregators(this.registerAggregators);
    }

    @Override
    public void close(Config config) {
        // Called when master close()
        this.aggregatorsHandler.clearAggregators();
        this.registerAggregators.clear();
    }

    @Override
    public void beforeSuperstep(Config config, int superstep) {
        /*
         * NOTE: rely on worker execute beforeSuperstep() to get all the
         * aggregators before this master beforeSuperstep() call.
         */
        this.aggregatorsHandler.resetAggregators(this.registerAggregators);
    }

    public AggregateRpcService handler() {
        return this.aggregatorsHandler;
    }

    public <V extends Value> void registerAggregator(String name,
                                                     Aggregator<V> aggr) {
        // Called when master init()
        this.registerAggregators.put(name, aggr);
    }

    public <V extends Value> void aggregatedAggregator(String name, V value) {
        // Called when master compute()
        E.checkArgument(value != null,
                        "Can't set value to null for aggregator '%s'", name);
        Aggregator<V> aggr = this.aggregatorsHandler.getAggregator(name);
        aggr.aggregatedValue(value);
    }

    public <V extends Value> V aggregatedValue(String name) {
        // Called when master compute()
        Aggregator<V> aggr = this.aggregatorsHandler.getAggregator(name);
        return aggr.aggregatedValue();
    }

    private class MasterAggregateHandler implements AggregateRpcService {

        private final Aggregators aggregators;

        public MasterAggregateHandler() {
            this.aggregators = new Aggregators();
        }

        @Override
        public RegisterAggregators registeredAggregators() {
            return MasterAggrManager.this.registerAggregators;
        }

        @Override
        public Map<String, Value> listAggregators() {
            return this.aggregators.values();
        }

        @Override
        public void aggregateAggregators(Map<String, Value> aggregators) {
            for (Entry<String, Value> aggr : aggregators.entrySet()) {
                this.aggregateAggregator(aggr.getKey(), aggr.getValue());
            }
            LOG.info("Master aggregate aggregators: {}", aggregators);
        }

        @Override
        public <V extends Value> Aggregator<V> getAggregator(String name) {
            Aggregator<?> aggr = this.aggregators.get(name, null);
            assert aggr != null;
            @SuppressWarnings("unchecked")
            Aggregator<V> aggregator = (Aggregator<V>) aggr;
            return aggregator;
        }

        @Override
        public <V extends Value> void aggregateAggregator(String name,
                                                          V value) {
            Aggregator<V> aggr = this.getAggregator(name);
            synchronized (aggr) {
                aggr.aggregateValue(value);
            }
        }

        public void resetAggregators(RegisterAggregators register) {
            this.aggregators.reset(register);
        }

        public void clearAggregators() {
            this.aggregators.clear();
        }
    }
}
