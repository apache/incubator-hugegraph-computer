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

import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.FloatValue;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.testutil.Assert;

public class MockComputation2 extends MockComputation {

    @Override
    protected void assertStat(WorkerContext context) {
        Assert.assertEquals(200L, context.totalVertexCount());
        Assert.assertEquals(400L, context.totalEdgeCount());
    }

    @Override
    protected void assertStep0Aggregators(WorkerContext context) {
        Assert.assertEquals(new IntValue(10), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_INT));
        Assert.assertEquals(new FloatValue(10.4f), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_FLOAT));

        Assert.assertEquals(new LongValue(10L), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_LONG_SUM));
        Assert.assertEquals(new LongValue(8L), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_LONG_MAX));

        Assert.assertEquals(new DoubleValue(20.8), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_DOUBLE_SUM));
        Assert.assertEquals(new DoubleValue(-10.0), context.aggregatedValue(
                            MockMasterComputation.AGGR_TEST_DOUBLE_MIN));
    }
}
