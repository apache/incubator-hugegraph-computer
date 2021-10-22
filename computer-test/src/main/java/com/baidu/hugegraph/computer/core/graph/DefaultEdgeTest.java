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

package com.baidu.hugegraph.computer.core.graph;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdge;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.properties.DefaultProperties;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;

public class DefaultEdgeTest extends UnitTestBase {

    @Test
    public void testConstructor() {
        DefaultEdge edge = new DefaultEdge(graphFactory());
        Assert.assertEquals(Constants.EMPTY_STR, edge.label());
        Assert.assertEquals(Constants.EMPTY_STR, edge.name());
        Assert.assertNull(edge.targetId());
        Assert.assertEquals(new DefaultProperties(graphFactory()),
                            edge.properties());

        edge = new DefaultEdge(graphFactory(), BytesId.of(1L), "knows",
                               "2021-06-01", BytesId.of(1L));
        Assert.assertEquals(BytesId.of(1L), edge.id());
        Assert.assertEquals("knows", edge.label());
        Assert.assertEquals("2021-06-01", edge.name());
        Assert.assertEquals(BytesId.of(1L), edge.targetId());
        Assert.assertEquals(new DefaultProperties(graphFactory()),
                            edge.properties());
    }

    @Test
    public void testOverwrite() {
        DefaultEdge edge = new DefaultEdge(graphFactory());
        edge.label("knows");
        edge.name("2021-06-01");
        edge.targetId(BytesId.of(1L));
        Properties properties = new DefaultProperties(graphFactory());
        properties.put("p1", new LongValue(1L));
        properties.put("p2", new DoubleValue(2.0D));
        edge.properties(properties);

        Assert.assertEquals("knows", edge.label());
        Assert.assertEquals("2021-06-01", edge.name());
        Assert.assertEquals(BytesId.of(1L), edge.targetId());
        Assert.assertEquals(properties, edge.properties());
    }

    @Test
    public void testEquals() {
        DefaultEdge edge1 = new DefaultEdge(graphFactory());
        edge1.id(BytesId.of(1L));
        edge1.label("knows");
        edge1.name("2021-06-01");
        edge1.targetId(BytesId.of(1L));
        Properties properties = new DefaultProperties(graphFactory());
        properties.put("p1", new LongValue(1L));
        properties.put("p2", new DoubleValue(2.0D));
        edge1.properties(properties);

        DefaultEdge edge2 = new DefaultEdge(graphFactory(), BytesId.of(1L),
                                            "knows", "2021-06-01",
                                            BytesId.of(1L));
        edge2.properties().put("p1", new LongValue(1L));
        edge2.properties().put("p2", new DoubleValue(2.0D));

        Assert.assertEquals(edge1, edge2);

        Assert.assertNotEquals(edge1, properties);
    }
}
