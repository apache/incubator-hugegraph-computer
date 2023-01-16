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

package org.apache.hugegraph.computer.core.graph;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.graph.edge.DefaultEdge;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.properties.DefaultProperties;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class DefaultEdgeTest extends UnitTestBase {

    @Test
    public void testConstructor() {
        DefaultEdge edge = new DefaultEdge(graphFactory());
        Assert.assertEquals(Constants.EMPTY_STR, edge.label());
        Assert.assertEquals(Constants.EMPTY_STR, edge.name());
        Assert.assertNull(edge.targetId());
        Assert.assertEquals(new DefaultProperties(graphFactory()),
                            edge.properties());

        edge = new DefaultEdge(graphFactory(), "knows", "2021-06-01",
                               BytesId.of(1L));
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
        edge1.label("knows");
        edge1.name("2021-06-01");
        edge1.targetId(BytesId.of(1L));
        Properties properties = new DefaultProperties(graphFactory());
        properties.put("p1", new LongValue(1L));
        properties.put("p2", new DoubleValue(2.0D));
        edge1.properties(properties);

        DefaultEdge edge2 = new DefaultEdge(graphFactory(), "knows",
                                            "2021-06-01", BytesId.of(1L));
        edge2.properties().put("p1", new LongValue(1L));
        edge2.properties().put("p2", new DoubleValue(2.0D));

        Assert.assertEquals(edge1, edge2);

        Assert.assertNotEquals(edge1, properties);
    }
}
