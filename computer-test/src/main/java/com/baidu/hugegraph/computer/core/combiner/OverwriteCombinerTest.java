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

package com.baidu.hugegraph.computer.core.combiner;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;

public class OverwriteCombinerTest extends UnitTestBase {

    @Test
    public void testCombineValue() {
        LongValue value1 = new LongValue(1L);
        LongValue value2 = new LongValue(2L);
        OverwriteCombiner<LongValue> combiner = new OverwriteCombiner<>();
        Value<?> value = combiner.combine(value1, value2);
        Assert.assertEquals(value2, value);
    }

    @Test
    public void testCombineNull() {
        LongValue value1 = new LongValue(1L);
        LongValue value2 = new LongValue(2L);
        OverwriteCombiner<LongValue> combiner = new OverwriteCombiner<>();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(null, value2);
        }, e -> {
            Assert.assertEquals("The combine parameter v1 can't be null",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(value1, null);
        }, e -> {
            Assert.assertEquals("The combine parameter v2 can't be null",
                                e.getMessage());
        });
    }

    @Test
    public void testCombineVertex() {
        GraphFactory factory = graphFactory();
        Id longId1 = BytesId.of(1L);
        DoubleValue value1 = new DoubleValue(0.1D);
        Vertex vertex1 = factory.createVertex(longId1, value1);
        vertex1.addEdge(factory.createEdge(BytesId.of(2L)));
        vertex1.addEdge(factory.createEdge(BytesId.of(3L)));

        Id longId2 = BytesId.of(1L);
        DoubleValue value2 = new DoubleValue(0.2D);
        Vertex vertex2 = factory.createVertex(longId2, value2);
        vertex2.addEdge(factory.createEdge(BytesId.of(1L)));

        OverwriteCombiner<Vertex> combiner = new OverwriteCombiner<>();
        Vertex vertex = combiner.combine(vertex1, vertex2);
        Assert.assertEquals(vertex2, vertex);
    }

    @Test
    public void testCombineProperties() {
        Properties properties1 = graphFactory().createProperties();
        properties1.put("name", BytesId.of("marko").idValue());
        properties1.put("city", BytesId.of("Beijing").idValue());

        Properties properties2 = graphFactory().createProperties();
        properties1.put("name", BytesId.of("josh").idValue());

        OverwriteCombiner<Properties> combiner = new OverwriteCombiner<>();
        Properties properties = combiner.combine(properties1, properties2);
        Assert.assertEquals(properties2, properties);
    }
}
