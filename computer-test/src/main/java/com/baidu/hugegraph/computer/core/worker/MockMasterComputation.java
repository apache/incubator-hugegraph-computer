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

package com.baidu.hugegraph.computer.core.worker;

import com.baidu.hugegraph.computer.core.aggregator.Aggregator;
import com.baidu.hugegraph.computer.core.combiner.DoubleValueSumCombiner;
import com.baidu.hugegraph.computer.core.combiner.LongValueSumCombiner;
import com.baidu.hugegraph.computer.core.combiner.ValueMaxCombiner;
import com.baidu.hugegraph.computer.core.combiner.ValueMinCombiner;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.FloatValue;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.master.DefaultMasterComputation;
import com.baidu.hugegraph.computer.core.master.MasterComputationContext;
import com.baidu.hugegraph.computer.core.master.MasterContext;
import com.baidu.hugegraph.testutil.Assert;

public class MockMasterComputation extends DefaultMasterComputation {

    public static final String AGGR_TEST_INT = "aggr_int";
    public static final String AGGR_TEST_FLOAT = "aggr_float";
    public static final String AGGR_TEST_MASTER = "aggr_float_unstable";

    public static final String AGGR_TEST_LONG_SUM = "aggr_long_sum";
    public static final String AGGR_TEST_LONG_MAX = "aggr_long_max";

    public static final String AGGR_TEST_DOUBLE_SUM = "aggr_double_sum";
    public static final String AGGR_TEST_DOUBLE_MIN = "aggr_double_min";

    @SuppressWarnings("unchecked")
    @Override
    public void init(MasterContext context) {
        context.registerAggregator(AGGR_TEST_INT, MockIntAggregator.class);
        context.registerAggregator(AGGR_TEST_FLOAT, MockFloatAggregator.class);
        context.registerAggregator(AGGR_TEST_MASTER, MockFloatAggregator.class);

        context.registerAggregator(AGGR_TEST_LONG_SUM, ValueType.LONG,
                                   LongValueSumCombiner.class);
        context.registerAggregator(AGGR_TEST_LONG_MAX, ValueType.LONG,
                                   ValueMaxCombiner.class);

        context.registerAggregator(AGGR_TEST_DOUBLE_SUM, ValueType.DOUBLE,
                                   DoubleValueSumCombiner.class);
        context.registerAggregator(AGGR_TEST_DOUBLE_MIN, ValueType.DOUBLE,
                                   ValueMinCombiner.class);
    }

    @Override
    public boolean compute(MasterComputationContext context) {
        this.assertStat(context);

        if (context.superstep() == 1) {
            this.assertStep0Aggregators(context);
        }

        return true;
    }

    protected void assertStat(MasterComputationContext context) {
        Assert.assertEquals(100L, context.totalVertexCount());
        Assert.assertEquals(200L, context.totalEdgeCount());
        Assert.assertEquals(50L, context.finishedVertexCount());
        Assert.assertEquals(60L, context.messageCount());
        Assert.assertEquals(70L, context.messageBytes());
    }

    protected void assertStep0Aggregators(MasterComputationContext context) {
        Assert.assertEquals(new IntValue(5), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_INT));
        Assert.assertEquals(new FloatValue(5.2f), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_FLOAT));
        Assert.assertEquals(new FloatValue(10.4f), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_MASTER));

        Assert.assertEquals(new LongValue(5L), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_LONG_SUM));
        Assert.assertEquals(new LongValue(8L), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_LONG_MAX));

        Assert.assertEquals(new DoubleValue(10.4), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_DOUBLE_SUM));
        Assert.assertEquals(new DoubleValue(-10.0), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_DOUBLE_MIN));

        context.aggregatedValue(MockMasterComputation.AGGR_TEST_MASTER,
                                new FloatValue(8.8f));
        Assert.assertEquals(new FloatValue(8.8f), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_MASTER));
    }

    public static class MockIntAggregator implements Aggregator<IntValue> {

        private IntValue value = new IntValue();

        @Override
        public void aggregateValue(int value) {
            this.value.value(this.value.value() + value);
        }

        @Override
        public void aggregateValue(IntValue value) {
            this.value.value(this.value.value() + value.value());
        }

        @Override
        public IntValue aggregatedValue() {
            return this.value;
        }

        @Override
        public void aggregatedValue(IntValue value) {
            this.value = value;
        }

        @Override
        public Aggregator<IntValue> copy() {
            MockIntAggregator copy = new MockIntAggregator();
            copy.value = this.value.copy();
            return copy;
        }

        @Override
        public void repair(ComputerContext context) {
            // pass
        }
    }

    public static class MockFloatAggregator implements Aggregator<FloatValue> {

        private FloatValue value = new FloatValue();

        @Override
        public void aggregateValue(float value) {
            this.value.value(this.value.value() + value);
        }

        @Override
        public void aggregateValue(FloatValue value) {
            this.value.value(this.value.value() + value.value());
        }

        @Override
        public FloatValue aggregatedValue() {
            return this.value;
        }

        @Override
        public void aggregatedValue(FloatValue value) {
            this.value = value;
        }

        @Override
        public Aggregator<FloatValue> copy() {
            MockFloatAggregator copy = new MockFloatAggregator();
            copy.value = this.value.copy();
            return copy;
        }

        @Override
        public void repair(ComputerContext context) {
            // pass
        }
    }
}
