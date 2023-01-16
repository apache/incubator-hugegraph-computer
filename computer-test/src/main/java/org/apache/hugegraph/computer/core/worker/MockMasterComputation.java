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

import org.apache.hugegraph.computer.core.aggregator.Aggregator;
import org.apache.hugegraph.computer.core.combiner.DoubleValueSumCombiner;
import org.apache.hugegraph.computer.core.combiner.FloatValueSumCombiner;
import org.apache.hugegraph.computer.core.combiner.IntValueSumCombiner;
import org.apache.hugegraph.computer.core.combiner.LongValueSumCombiner;
import org.apache.hugegraph.computer.core.combiner.ValueMaxCombiner;
import org.apache.hugegraph.computer.core.combiner.ValueMinCombiner;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.FloatValue;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.master.DefaultMasterComputation;
import org.apache.hugegraph.computer.core.master.MasterComputationContext;
import org.apache.hugegraph.computer.core.master.MasterContext;
import org.apache.hugegraph.testutil.Assert;

public class MockMasterComputation extends DefaultMasterComputation {

    public static final String AGGR_CUSTOM_INT = "aggr_int";
    public static final String AGGR_CUSTOM_FLOAT = "aggr_float";

    public static final String AGGR_FLOAT_UNSTABLE = "aggr_float_unstable";
    public static final String AGGR_INT_UNSTABLE = "aggr_int_unstable";

    public static final String AGGR_INT_SUM = "aggr_int_sum";
    public static final String AGGR_INT_MAX = "aggr_int_max";

    public static final String AGGR_LONG_SUM = "aggr_long_sum";
    public static final String AGGR_LONG_MAX = "aggr_long_max";

    public static final String AGGR_FLOAT_SUM = "aggr_float_sum";
    public static final String AGGR_FLOAT_MIN = "aggr_float_min";

    public static final String AGGR_DOUBLE_SUM = "aggr_double_sum";
    public static final String AGGR_DOUBLE_MIN = "aggr_double_min";

    @SuppressWarnings("unchecked")
    @Override
    public void init(MasterContext context) {
        context.registerAggregator(AGGR_CUSTOM_INT, MockIntAggregator.class);
        context.registerAggregator(AGGR_CUSTOM_FLOAT,
                                   MockFloatAggregator.class);

        context.registerAggregator(AGGR_FLOAT_UNSTABLE,
                                   MockFloatAggregator.class);

        context.registerAggregator(AGGR_INT_UNSTABLE,
                                   new IntValue(0),
                                   ValueMinCombiner.class);
        context.registerAggregator(AGGR_INT_UNSTABLE, // overwrite is ok
                                   new IntValue(Integer.MAX_VALUE),
                                   ValueMinCombiner.class);

        context.registerAggregator(AGGR_INT_SUM, ValueType.INT,
                                   IntValueSumCombiner.class);
        context.registerAggregator(AGGR_INT_MAX, ValueType.INT,
                                   ValueMaxCombiner.class);

        context.registerAggregator(AGGR_LONG_SUM, ValueType.LONG,
                                   LongValueSumCombiner.class);
        context.registerAggregator(AGGR_LONG_MAX, ValueType.LONG,
                                   ValueMaxCombiner.class);

        context.registerAggregator(AGGR_FLOAT_SUM, ValueType.FLOAT,
                                   FloatValueSumCombiner.class);
        context.registerAggregator(AGGR_FLOAT_MIN, ValueType.FLOAT,
                                   ValueMinCombiner.class);

        context.registerAggregator(AGGR_DOUBLE_SUM, ValueType.DOUBLE,
                                   DoubleValueSumCombiner.class);
        context.registerAggregator(AGGR_DOUBLE_MIN, ValueType.DOUBLE,
                                   ValueMinCombiner.class);

        this.registerAggregatorWithError(context);
    }

    private void registerAggregatorWithError(MasterContext context) {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.registerAggregator("", MockIntAggregator.class);
        }, e -> {
            Assert.assertContains("registered aggregator name can't be empty",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.registerAggregator(null, MockIntAggregator.class);
        }, e -> {
            Assert.assertContains("registered aggregator name can't be null",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.registerAggregator("", ValueType.INT,
                                       IntValueSumCombiner.class);
        }, e -> {
            Assert.assertContains("registered aggregator name can't be empty",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.registerAggregator(null, ValueType.INT,
                                       IntValueSumCombiner.class);
        }, e -> {
            Assert.assertContains("registered aggregator name can't be null",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.registerAggregator(AGGR_INT_UNSTABLE, ValueType.INT, null);
        }, e -> {
            Assert.assertContains("combiner of aggregator can't be null",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            ValueType type = null;
            context.registerAggregator(AGGR_INT_UNSTABLE, type,
                                       IntValueSumCombiner.class);
        }, e -> {
            Assert.assertContains("value type of aggregator can't be null",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            IntValue value = null;
            context.registerAggregator(AGGR_INT_UNSTABLE, value,
                                       IntValueSumCombiner.class);
        }, e -> {
            Assert.assertContains("The aggregator default value can't be null",
                                  e.getMessage());
        });

        // Not applied now, can get it through aggregatedValue() after inited()
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.aggregatedValue(AGGR_INT_UNSTABLE,
                                    new IntValue(Integer.MAX_VALUE));
        }, e -> {
            Assert.assertContains("Can't get aggregator 'aggr_int_unstable",
                                  e.getMessage());
        });
    }

    @Override
    public boolean compute(MasterComputationContext context) {
        if (context.superstep() == 0) {
            this.assertStep0Aggregators(context);
            this.updateStep0Aggregators(context);
        } else if (context.superstep() == 1) {
            this.assertStep1Aggregators(context);
        }

        return true;
    }

    protected void assertStat(MasterComputationContext context) {
        Assert.assertEquals(6L, context.totalVertexCount());
        Assert.assertEquals(5L, context.totalEdgeCount());
        Assert.assertEquals(0L, context.finishedVertexCount());
        Assert.assertEquals(0L, context.messageCount());
        Assert.assertEquals(0L, context.messageBytes());
    }

    protected void assertStep0Aggregators(MasterComputationContext context) {
        Assert.assertEquals(new IntValue(5), context.aggregatedValue(
                            MockMasterComputation.AGGR_CUSTOM_INT));
        Assert.assertEquals(new FloatValue(5.2f), context.aggregatedValue(
                            MockMasterComputation.AGGR_CUSTOM_FLOAT));

        Assert.assertEquals(new FloatValue(3.14f), context.aggregatedValue(
                            MockMasterComputation.AGGR_FLOAT_UNSTABLE));
        Assert.assertEquals(new IntValue(10),
                            context.aggregatedValue(
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

    protected void assertStep1Aggregators(MasterComputationContext context) {
        Assert.assertEquals(new IntValue(5), context.aggregatedValue(
                            MockMasterComputation.AGGR_CUSTOM_INT));
        Assert.assertEquals(new FloatValue(5.2f), context.aggregatedValue(
                            MockMasterComputation.AGGR_CUSTOM_FLOAT));

        Assert.assertEquals(new FloatValue(3.14f), context.aggregatedValue(
                            MockMasterComputation.AGGR_FLOAT_UNSTABLE));
        Assert.assertEquals(new IntValue(9), context.aggregatedValue(
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

    protected void updateStep0Aggregators(MasterComputationContext context) {
        // Update UNSTABLE aggregator
        context.aggregatedValue(MockMasterComputation.AGGR_FLOAT_UNSTABLE,
                                new FloatValue(8.8f));
        Assert.assertEquals(new FloatValue(8.8f), context.aggregatedValue(
                            MockMasterComputation.AGGR_FLOAT_UNSTABLE));

        context.aggregatedValue(MockMasterComputation.AGGR_INT_UNSTABLE,
                                new IntValue(888));
        Assert.assertEquals(new IntValue(888), context.aggregatedValue(
                            MockMasterComputation.AGGR_INT_UNSTABLE));

        // Update aggregator with error
        this.assertAggregatedValueWithError(context);
    }

    private void assertAggregatedValueWithError(MasterComputationContext
                                                context) {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.aggregatedValue(MockMasterComputation.AGGR_INT_SUM,
                                    new LongValue(7L));
        }, e -> {
            Assert.assertContains("Can't set long value '7' to int aggregator",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.aggregatedValue(MockMasterComputation.AGGR_LONG_SUM,
                                    new IntValue(7));
        }, e -> {
            Assert.assertContains("Can't set int value '7' to long aggregator",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.aggregatedValue(MockMasterComputation.AGGR_DOUBLE_SUM,
                                    new FloatValue(7f));
        }, e -> {
            Assert.assertContains("Can't set float value '7.0' to double ",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.aggregatedValue(MockMasterComputation.AGGR_DOUBLE_MIN,
                                    null);
        }, e -> {
            Assert.assertContains("Can't set value to null for aggregator " +
                                  "'aggr_double_min'", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            context.aggregatedValue(MockMasterComputation.AGGR_CUSTOM_INT,
                                    null);
        }, e -> {
            Assert.assertContains("Can't set value to null for aggregator " +
                                  "'aggr_int'", e.getMessage());
        });

        Assert.assertThrows(ClassCastException.class, () -> {
            context.aggregatedValue(MockMasterComputation.AGGR_CUSTOM_FLOAT,
                                    new IntValue(7));
        }, e -> {
            Assert.assertContains("IntValue cannot be cast to", e.getMessage());
            Assert.assertContains("FloatValue", e.getMessage());
        });
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
