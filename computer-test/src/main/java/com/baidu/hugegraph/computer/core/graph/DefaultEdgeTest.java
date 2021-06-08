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

import java.io.IOException;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdge;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
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
        Assert.assertEquals(new DefaultProperties(graphFactory(),
                                                  valueFactory()),
                            edge.properties());

        edge = new DefaultEdge(graphFactory(), "knows", "2021-06-01",
                               new LongId(1L));
        Assert.assertEquals("knows", edge.label());
        Assert.assertEquals("2021-06-01", edge.name());
        Assert.assertEquals(new LongId(1L), edge.targetId());
        Assert.assertEquals(new DefaultProperties(graphFactory(),
                                                  valueFactory()),
                            edge.properties());
    }

    @Test
    public void testOverwrite() {
        DefaultEdge edge = new DefaultEdge(graphFactory());
        edge.label("knows");
        edge.name("2021-06-01");
        edge.targetId(new LongId(1L));
        Properties properties = new DefaultProperties(graphFactory(),
                                                      valueFactory());
        properties.put("p1", new LongValue(1L));
        properties.put("p2", new DoubleValue(2.0D));
        edge.properties(properties);

        Assert.assertEquals("knows", edge.label());
        Assert.assertEquals("2021-06-01", edge.name());
        Assert.assertEquals(new LongId(1L), edge.targetId());
        Assert.assertEquals(properties, edge.properties());
    }

    @Test
    public void testEquals() throws IOException {
        DefaultEdge edge1 = new DefaultEdge(graphFactory());
        edge1.label("knows");
        edge1.name("2021-06-01");
        edge1.targetId(new LongId(1L));
        Properties properties = new DefaultProperties(graphFactory(),
                                                      valueFactory());
        properties.put("p1", new LongValue(1L));
        properties.put("p2", new DoubleValue(2.0D));
        edge1.properties(properties);

        DefaultEdge edge2 = new DefaultEdge(graphFactory(), "knows",
                                            "2021-06-01", new LongId(1L));
        edge2.properties().put("p1", new LongValue(1L));
        edge2.properties().put("p2", new DoubleValue(2.0D));

        Assert.assertEquals(edge1, edge2);

        Assert.assertNotEquals(edge1, properties);
    }
}
