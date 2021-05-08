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

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.aggregator.Aggregator;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.FloatValue;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.testutil.Assert;

public class MockComputation implements Computation<DoubleValue> {

    private Aggregator<Value<?>> aggrInt;
    private Aggregator<Value<?>> aggrFloat;

    private Aggregator<Value<?>> aggrLongSum;
    private Aggregator<Value<?>> aggrLongMax;

    private Aggregator<Value<?>> aggrDoubleSum;
    private Aggregator<Value<?>> aggrDoubleMin;

    @Override
    public void beforeSuperstep(WorkerContext context) {
        this.assertAggregators(context);
        if (context.superstep() == 1) {
            this.assertStep0Aggregators(context);
        }

        Assert.assertEquals(100L, context.totalVertexCount());
        Assert.assertEquals(200L, context.totalEdgeCount());
    }

    @Override
    public void afterSuperstep(WorkerContext context) {
        context.aggregateValue(MockMasterComputation.AGGR_TEST_INT,
                               this.aggrInt.aggregatedValue());
        context.aggregateValue(MockMasterComputation.AGGR_TEST_FLOAT,
                               this.aggrFloat.aggregatedValue());

        context.aggregateValue(MockMasterComputation.AGGR_TEST_LONG_SUM,
                               this.aggrLongSum.aggregatedValue());
        context.aggregateValue(MockMasterComputation.AGGR_TEST_LONG_MAX,
                               this.aggrLongMax.aggregatedValue());

        context.aggregateValue(MockMasterComputation.AGGR_TEST_DOUBLE_SUM,
                               this.aggrDoubleSum.aggregatedValue());
        context.aggregateValue(MockMasterComputation.AGGR_TEST_DOUBLE_MIN,
                               this.aggrDoubleMin.aggregatedValue());
    }

    private void assertAggregators(WorkerContext context) {
        // AGGR_TEST_INT
        this.aggrInt = context.createAggregator(
                       MockMasterComputation.AGGR_TEST_INT);

        Assert.assertEquals(new IntValue(0),
                            this.aggrInt.aggregatedValue());

        this.aggrInt.aggregateValue(1);
        Assert.assertEquals(new IntValue(1),
                            this.aggrInt.aggregatedValue());

        this.aggrInt.aggregateValue(new IntValue(1));
        Assert.assertEquals(new IntValue(2),
                            this.aggrInt.aggregatedValue());

        this.aggrInt.aggregateValue(3);
        Assert.assertEquals(new IntValue(5),
                            this.aggrInt.aggregatedValue());

        // AGGR_TEST_FLOAT
        this.aggrFloat = context.createAggregator(
                         MockMasterComputation.AGGR_TEST_FLOAT);

        Assert.assertEquals(new FloatValue(0),
                            this.aggrFloat.aggregatedValue());

        this.aggrFloat.aggregateValue(1f);
        Assert.assertEquals(new FloatValue(1),
                            this.aggrFloat.aggregatedValue());

        this.aggrFloat.aggregateValue(new FloatValue(1));
        Assert.assertEquals(new FloatValue(2),
                            this.aggrFloat.aggregatedValue());

        this.aggrFloat.aggregateValue(3.2f);
        Assert.assertEquals(new FloatValue(5.2f),
                            this.aggrFloat.aggregatedValue());

        // AGGR_TEST_LONG_SUM
        this.aggrLongSum = context.createAggregator(
                           MockMasterComputation.AGGR_TEST_LONG_SUM);

        Assert.assertEquals(new LongValue(0L),
                            this.aggrLongSum.aggregatedValue());

        this.aggrLongSum.aggregateValue(1L);
        Assert.assertEquals(new LongValue(1L),
                            this.aggrLongSum.aggregatedValue());

        this.aggrLongSum.aggregateValue(new LongValue(1L));
        Assert.assertEquals(new LongValue(2L),
                            this.aggrLongSum.aggregatedValue());

        this.aggrLongSum.aggregateValue(3L);
        Assert.assertEquals(new LongValue(5L),
                            this.aggrLongSum.aggregatedValue());

        // AGGR_TEST_LONG_MAX
        this.aggrLongMax = context.createAggregator(
                           MockMasterComputation.AGGR_TEST_LONG_MAX);

        Assert.assertEquals(new LongValue(0L),
                            this.aggrLongMax.aggregatedValue());

        this.aggrLongMax.aggregateValue(1L);
        Assert.assertEquals(new LongValue(1L),
                            this.aggrLongMax.aggregatedValue());

        this.aggrLongMax.aggregateValue(8L);
        Assert.assertEquals(new LongValue(8L),
                            this.aggrLongMax.aggregatedValue());

        this.aggrLongMax.aggregateValue(3L);
        Assert.assertEquals(new LongValue(8L),
                            this.aggrLongMax.aggregatedValue());

        // AGGR_TEST_DOUBLE_SUM
        this.aggrDoubleSum = context.createAggregator(
                             MockMasterComputation.AGGR_TEST_DOUBLE_SUM);

        Assert.assertEquals(new DoubleValue(0),
                            this.aggrDoubleSum.aggregatedValue());

        this.aggrDoubleSum.aggregateValue(1.1);
        Assert.assertEquals(new DoubleValue(1.1),
                            this.aggrDoubleSum.aggregatedValue());

        this.aggrDoubleSum.aggregateValue(2.3);
        Assert.assertEquals(new DoubleValue(3.4),
                            this.aggrDoubleSum.aggregatedValue());

        this.aggrDoubleSum.aggregateValue(7.0);
        Assert.assertEquals(new DoubleValue(10.4),
                            this.aggrDoubleSum.aggregatedValue());

        // AGGR_TEST_DOUBLE_MIN
        this.aggrDoubleMin = context.createAggregator(
                             MockMasterComputation.AGGR_TEST_DOUBLE_MIN);

        Assert.assertEquals(new DoubleValue(0),
                            this.aggrDoubleMin.aggregatedValue());

        this.aggrDoubleMin.aggregateValue(1.1);
        Assert.assertEquals(new DoubleValue(0),
                            this.aggrDoubleMin.aggregatedValue());

        this.aggrDoubleMin.aggregateValue(-10.0);
        Assert.assertEquals(new DoubleValue(-10.0),
                            this.aggrDoubleMin.aggregatedValue());

        this.aggrDoubleMin.aggregateValue(-4.0);
        Assert.assertEquals(new DoubleValue(-10.0),
                            this.aggrDoubleMin.aggregatedValue());
    }

    private void assertStep0Aggregators(WorkerContext context) {
        Assert.assertEquals(new IntValue(5), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_INT));
        Assert.assertEquals(new FloatValue(5.2f), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_FLOAT));

        Assert.assertEquals(new LongValue(5L), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_LONG_SUM));
        Assert.assertEquals(new LongValue(8L), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_LONG_MAX));

        Assert.assertEquals(new DoubleValue(10.4), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_DOUBLE_SUM));
        Assert.assertEquals(new DoubleValue(-10.0), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_DOUBLE_MIN));
    }

    @Override
    public String name() {
        return "mock";
    }

    @Override
    public String category() {
        return "mock";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        // pass
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<DoubleValue> messages) {
        // pass
    }
}
