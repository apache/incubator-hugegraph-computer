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

package org.apache.hugegraph.computer.core.combiner;

import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class IntValueSumCombinerTest {

    @Test
    public void testCombine() {
        IntValue sum = new IntValue(0);
        IntValueSumCombiner combiner = new IntValueSumCombiner();
        for (int i = 1; i <= 10; i++) {
            IntValue value = new IntValue(i);
            combiner.combine(sum, value, sum);
        }
        Assert.assertEquals(55, sum.value());
    }

    @Test
    public void testCombineNull() {
        IntValue value1 = new IntValue(1);
        IntValue value2 = new IntValue(2);
        IntValueSumCombiner combiner = new IntValueSumCombiner();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(null, value2, value2);
        }, e -> {
            Assert.assertEquals("The combine parameter v1 can't be null",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(value1, null, value2);
        }, e -> {
            Assert.assertEquals("The combine parameter v2 can't be null",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(value1, value2, null);
        }, e -> {
            Assert.assertEquals("The combine parameter result can't be null",
                                e.getMessage());
        });
    }
}
