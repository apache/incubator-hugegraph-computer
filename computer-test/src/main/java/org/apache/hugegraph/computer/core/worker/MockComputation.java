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

package org.apache.hugegraph.computer.core.worker;

import java.util.Iterator;

import org.apache.hugegraph.computer.core.aggregator.Aggregator;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.FloatValue;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.testutil.Assert;

public class MockComputation implements Computation<DoubleValue> {

    private Aggregator<Value> aggrCustomInt;
    private Aggregator<Value> aggrCustomFloat;

    private Aggregator<Value> aggrIntSum;
    private Aggregator<Value> aggrIntMax;

    private Aggregator<Value> aggrLongSum;
    private Aggregator<Value> aggrLongMax;

    private Aggregator<Value> aggrFloatSum;
    private Aggregator<Value> aggrFloatMin;

    private Aggregator<Value> aggrDoubleSum;
    private Aggregator<Value> aggrDoubleMin;

    @Override
    public void beforeSuperstep(WorkerContext context) {
        this.createAndRunAggregators(context);

        if (context.superstep() == 0) {
            this.assertStep0Aggregators(context);
        } else if (context.superstep() == 1) {
            this.assertStep1Aggregators(context);
        }
    }

    @Override
    public void afterSuperstep(WorkerContext context) {
        context.aggregateValue(MockMasterComputation.AGGR_CUSTOM_INT,
                               this.aggrCustomInt.aggregatedValue());
        context.aggregateValue(MockMasterComputation.AGGR_CUSTOM_FLOAT,
                               this.aggrCustomFloat.aggregatedValue());

        context.aggregateValue(MockMasterComputation.AGGR_FLOAT_UNSTABLE,
                               new FloatValue(3.14f));
        context.aggregateValue(MockMasterComputation.AGGR_INT_UNSTABLE,
                               new IntValue(10 - context.superstep()));

        context.aggregateValue(MockMasterComputation.AGGR_INT_SUM,
                               this.aggrIntSum.aggregatedValue());
        context.aggregateValue(MockMasterComputation.AGGR_INT_MAX,
                               this.aggrIntMax.aggregatedValue());

        context.aggregateValue(MockMasterComputation.AGGR_LONG_SUM,
                               this.aggrLongSum.aggregatedValue());
        context.aggregateValue(MockMasterComputation.AGGR_LONG_MAX,
                               this.aggrLongMax.aggregatedValue());

        context.aggregateValue(MockMasterComputation.AGGR_FLOAT_SUM,
                               this.aggrFloatSum.aggregatedValue());
        context.aggregateValue(MockMasterComputation.AGGR_FLOAT_MIN,
                               this.aggrFloatMin.aggregatedValue());

        context.aggregateValue(MockMasterComputation.AGGR_DOUBLE_SUM,
                               this.aggrDoubleSum.aggregatedValue());
        context.aggregateValue(MockMasterComputation.AGGR_DOUBLE_MIN,
                               this.aggrDoubleMin.aggregatedValue());

        this.assertAggregateValueWithError(context);
    }

    private void assertAggregateValueWithError(WorkerContext context) {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.aggregateValue(MockMasterComputation.AGGR_INT_SUM,
                                   this.aggrLongSum.aggregatedValue());
        }, e -> {
            Assert.assertContains("Can't set long value '5' to int aggregator",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.aggregateValue(MockMasterComputation.AGGR_LONG_SUM,
                                   this.aggrIntSum.aggregatedValue());
        }, e -> {
            Assert.assertContains("Can't set int value '5' to long aggregator",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.aggregateValue(MockMasterComputation.AGGR_DOUBLE_SUM,
                                   this.aggrFloatSum.aggregatedValue());
        }, e -> {
            Assert.assertContains("Can't set float value '10.4' to double ",
                                  e.getMessage());
        });

        Assert.assertThrows(ClassCastException.class, () -> {
            context.aggregateValue(MockMasterComputation.AGGR_CUSTOM_FLOAT,
                                   new IntValue(7));
        }, e -> {
            Assert.assertContains("IntValue cannot be cast to", e.getMessage());
            Assert.assertContains("FloatValue", e.getMessage());
        });
    }

    protected void createAndRunAggregators(WorkerContext context) {
        // AGGR_CUSTOM_INT
        this.aggrCustomInt = context.createAggregator(
                             MockMasterComputation.AGGR_CUSTOM_INT);

        Assert.assertEquals(new IntValue(0),
                            this.aggrCustomInt.aggregatedValue());

        this.aggrCustomInt.aggregateValue(1);
        Assert.assertEquals(new IntValue(1),
                            this.aggrCustomInt.aggregatedValue());

        this.aggrCustomInt.aggregateValue(new IntValue(1));
        Assert.assertEquals(new IntValue(2),
                            this.aggrCustomInt.aggregatedValue());

        this.aggrCustomInt.aggregateValue(3);
        Assert.assertEquals(new IntValue(5),
                            this.aggrCustomInt.aggregatedValue());

        // AGGR_CUSTOM_FLOAT
        this.aggrCustomFloat = context.createAggregator(
                               MockMasterComputation.AGGR_CUSTOM_FLOAT);

        Assert.assertEquals(new FloatValue(0f),
                            this.aggrCustomFloat.aggregatedValue());

        this.aggrCustomFloat.aggregateValue(1f);
        Assert.assertEquals(new FloatValue(1f),
                            this.aggrCustomFloat.aggregatedValue());

        this.aggrCustomFloat.aggregateValue(new FloatValue(1f));
        Assert.assertEquals(new FloatValue(2f),
                            this.aggrCustomFloat.aggregatedValue());

        this.aggrCustomFloat.aggregateValue(3.2f);
        Assert.assertEquals(new FloatValue(5.2f),
                            this.aggrCustomFloat.aggregatedValue());

        // AGGR_INT_SUM
        this.aggrIntSum = context.createAggregator(
                          MockMasterComputation.AGGR_INT_SUM);

        Assert.assertEquals(new IntValue(0),
                            this.aggrIntSum.aggregatedValue());

        this.aggrIntSum.aggregateValue(1);
        Assert.assertEquals(new IntValue(1),
                            this.aggrIntSum.aggregatedValue());

        this.aggrIntSum.aggregateValue(new IntValue(1));
        Assert.assertEquals(new IntValue(2),
                            this.aggrIntSum.aggregatedValue());

        this.aggrIntSum.aggregateValue(3);
        Assert.assertEquals(new IntValue(5),
                            this.aggrIntSum.aggregatedValue());

        // AGGR_INT_MAX
        this.aggrIntMax = context.createAggregator(
                          MockMasterComputation.AGGR_INT_MAX);

        Assert.assertEquals(new IntValue(0),
                            this.aggrIntMax.aggregatedValue());

        this.aggrIntMax.aggregateValue(1);
        Assert.assertEquals(new IntValue(1),
                            this.aggrIntMax.aggregatedValue());

        this.aggrIntMax.aggregateValue(8);
        Assert.assertEquals(new IntValue(8),
                            this.aggrIntMax.aggregatedValue());

        this.aggrIntMax.aggregateValue(3);
        Assert.assertEquals(new IntValue(8),
                            this.aggrIntMax.aggregatedValue());

        // AGGR_LONG_SUM
        this.aggrLongSum = context.createAggregator(
                           MockMasterComputation.AGGR_LONG_SUM);

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

        // AGGR_LONG_MAX
        this.aggrLongMax = context.createAggregator(
                           MockMasterComputation.AGGR_LONG_MAX);

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

        // AGGR_FLOAT_SUM
        this.aggrFloatSum = context.createAggregator(
                            MockMasterComputation.AGGR_FLOAT_SUM);

        Assert.assertEquals(new FloatValue(0.0f),
                            this.aggrFloatSum.aggregatedValue());

        this.aggrFloatSum.aggregateValue(1.1f);
        Assert.assertEquals(new FloatValue(1.1f),
                            this.aggrFloatSum.aggregatedValue());

        this.aggrFloatSum.aggregateValue(2.3f);
        Assert.assertEquals(new FloatValue(3.4f),
                            this.aggrFloatSum.aggregatedValue());

        this.aggrFloatSum.aggregateValue(7.0f);
        Assert.assertEquals(new FloatValue(10.4f),
                            this.aggrFloatSum.aggregatedValue());

        // AGGR_FLOAT_MIN
        this.aggrFloatMin = context.createAggregator(
                            MockMasterComputation.AGGR_FLOAT_MIN);

        Assert.assertEquals(new FloatValue(0.0f),
                            this.aggrFloatMin.aggregatedValue());

        this.aggrFloatMin.aggregateValue(1.1f);
        Assert.assertEquals(new FloatValue(0.0f),
                            this.aggrFloatMin.aggregatedValue());

        this.aggrFloatMin.aggregateValue(-10.0f);
        Assert.assertEquals(new FloatValue(-10.0f),
                            this.aggrFloatMin.aggregatedValue());

        this.aggrFloatMin.aggregateValue(-4.0f);
        Assert.assertEquals(new FloatValue(-10.0f),
                            this.aggrFloatMin.aggregatedValue());

        // AGGR_DOUBLE_SUM
        this.aggrDoubleSum = context.createAggregator(
                             MockMasterComputation.AGGR_DOUBLE_SUM);

        Assert.assertEquals(new DoubleValue(0.0),
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

        // AGGR_DOUBLE_MIN
        this.aggrDoubleMin = context.createAggregator(
                             MockMasterComputation.AGGR_DOUBLE_MIN);

        Assert.assertEquals(new DoubleValue(0.0),
                            this.aggrDoubleMin.aggregatedValue());

        this.aggrDoubleMin.aggregateValue(1.1);
        Assert.assertEquals(new DoubleValue(0.0),
                            this.aggrDoubleMin.aggregatedValue());

        this.aggrDoubleMin.aggregateValue(-10.0);
        Assert.assertEquals(new DoubleValue(-10.0),
                            this.aggrDoubleMin.aggregatedValue());

        this.aggrDoubleMin.aggregateValue(-4.0);
        Assert.assertEquals(new DoubleValue(-10.0),
                            this.aggrDoubleMin.aggregatedValue());
    }

    protected void assertStep0Aggregators(WorkerContext context) {
        Assert.assertEquals(new IntValue(0), context.aggregatedValue(
                            MockMasterComputation.AGGR_CUSTOM_INT));
        Assert.assertEquals(new FloatValue(0f), context.aggregatedValue(
                            MockMasterComputation.AGGR_CUSTOM_FLOAT));

        Assert.assertEquals(new FloatValue(0f), context.aggregatedValue(
                            MockMasterComputation.AGGR_FLOAT_UNSTABLE));
        Assert.assertEquals(new IntValue(Integer.MAX_VALUE),
                            context.aggregatedValue(
                            MockMasterComputation.AGGR_INT_UNSTABLE));

        Assert.assertEquals(new IntValue(0), context.aggregatedValue(
                            MockMasterComputation.AGGR_INT_SUM));
        Assert.assertEquals(new IntValue(0), context.aggregatedValue(
                            MockMasterComputation.AGGR_INT_MAX));

        Assert.assertEquals(new LongValue(0L), context.aggregatedValue(
                            MockMasterComputation.AGGR_LONG_SUM));
        Assert.assertEquals(new LongValue(0L), context.aggregatedValue(
                            MockMasterComputation.AGGR_LONG_MAX));

        Assert.assertEquals(new FloatValue(0f), context.aggregatedValue(
                            MockMasterComputation.AGGR_FLOAT_SUM));
        Assert.assertEquals(new FloatValue(0f), context.aggregatedValue(
                            MockMasterComputation.AGGR_FLOAT_MIN));

        Assert.assertEquals(new DoubleValue(0d), context.aggregatedValue(
                            MockMasterComputation.AGGR_DOUBLE_SUM));
        Assert.assertEquals(new DoubleValue(0d), context.aggregatedValue(
                            MockMasterComputation.AGGR_DOUBLE_MIN));
    }

    protected void assertStep1Aggregators(WorkerContext context) {
        Assert.assertEquals(new IntValue(5), context.aggregatedValue(
                            MockMasterComputation.AGGR_CUSTOM_INT));
        Assert.assertEquals(new FloatValue(5.2f), context.aggregatedValue(
                            MockMasterComputation.AGGR_CUSTOM_FLOAT));

        Assert.assertEquals(new FloatValue(8.8f), context.aggregatedValue(
                            MockMasterComputation.AGGR_FLOAT_UNSTABLE));
        Assert.assertEquals(new IntValue(888), context.aggregatedValue(
                            MockMasterComputation.AGGR_INT_UNSTABLE));

        Assert.assertEquals(new IntValue(5), context.aggregatedValue(
                            MockMasterComputation.AGGR_INT_SUM));
        Assert.assertEquals(new IntValue(8), context.aggregatedValue(
                            MockMasterComputation.AGGR_INT_MAX));

        Assert.assertEquals(new LongValue(5L), context.aggregatedValue(
                            MockMasterComputation.AGGR_LONG_SUM));
        Assert.assertEquals(new LongValue(8L), context.aggregatedValue(
                            MockMasterComputation.AGGR_LONG_MAX));

        Assert.assertEquals(new FloatValue(10.4f), context.aggregatedValue(
                            MockMasterComputation.AGGR_FLOAT_SUM));
        Assert.assertEquals(new FloatValue(-10.0f), context.aggregatedValue(
                            MockMasterComputation.AGGR_FLOAT_MIN));

        Assert.assertEquals(new DoubleValue(10.4), context.aggregatedValue(
                            MockMasterComputation.AGGR_DOUBLE_SUM));
        Assert.assertEquals(new DoubleValue(-10.0), context.aggregatedValue(
                            MockMasterComputation.AGGR_DOUBLE_MIN));
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
        vertex.value(new DoubleValue(0.5D));
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<DoubleValue> messages) {
        // pass
    }
}
