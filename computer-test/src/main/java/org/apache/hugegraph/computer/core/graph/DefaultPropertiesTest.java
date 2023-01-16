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

import java.io.IOException;
import java.util.Map;

import org.apache.hugegraph.computer.core.graph.properties.DefaultProperties;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class DefaultPropertiesTest extends UnitTestBase {

    @Test
    public void testConstructor() {
        DefaultProperties properties = new DefaultProperties(graphFactory());
        Assert.assertEquals(0, properties.get().size());
        properties.put("p1", new LongValue(1L));
        properties.put("p2", new DoubleValue(2.0D));

        Map<String, Value> props = properties.get();
        Assert.assertEquals(2, props.size());
        Assert.assertEquals(new LongValue(1L), props.get("p1"));
        Assert.assertEquals(new DoubleValue(2.0D), props.get("p2"));
    }

    @Test
    public void testOverwrite() {
        DefaultProperties properties = new DefaultProperties(graphFactory());
        Assert.assertEquals(0, properties.get().size());
        properties.put("p1", new LongValue(1L));
        properties.put("p2", new DoubleValue(2.0D));

        Assert.assertEquals(new LongValue(1L), properties.get("p1"));
        properties.put("p1", new LongValue(2L));
        Assert.assertEquals(new LongValue(2L), properties.get("p1"));

        Map<String, Value> props = properties.get();
        Assert.assertEquals(2, props.size());
        Assert.assertEquals(new LongValue(2L), props.get("p1"));
        Assert.assertEquals(new DoubleValue(2.0D), props.get("p2"));
    }

    @Test
    public void testReadWrite() throws IOException {
        DefaultProperties properties = new DefaultProperties(graphFactory());
        Assert.assertEquals(0, properties.get().size());
        properties.put("p1", new LongValue(1L));
        properties.put("p2", new DoubleValue(2.0D));

        DefaultProperties props2 = new DefaultProperties(graphFactory());
        UnitTestBase.assertEqualAfterWriteAndRead(properties, props2);
    }

    @Test
    public void testEquals() {
        DefaultProperties props1 = new DefaultProperties(graphFactory());
        props1.put("p1", new LongValue(1L));
        props1.put("p2", new DoubleValue(2.0D));

        DefaultProperties props2 = new DefaultProperties(
                                   props1.get(),
                                   UnitTestBase.graphFactory());
        Assert.assertEquals(props1, props2);
    }

    @Test
    public void testHashCode() {
        DefaultProperties props = new DefaultProperties(graphFactory());
        props.put("p1", new LongValue(1L));
        props.put("p2", new DoubleValue(2.0D));

        Assert.assertEquals(1073748897, props.hashCode());
    }
}
