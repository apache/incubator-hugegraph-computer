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

package com.baidu.hugegraph.computer.core.sender;

import java.io.IOException;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.id.Utf8Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.ListValue;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;

public class WriteBufferTest extends UnitTestBase {

    private final ComputerContext context = ComputerContext.instance();

    @Test
    public void testConstructor() {
        Assert.assertThrows(AssertionError.class, () -> {
            new WriteBuffer(context, 0, 20);
        });
        Assert.assertThrows(AssertionError.class, () -> {
            new WriteBuffer(context, 10, -1);
        });
        Assert.assertThrows(AssertionError.class, () -> {
            new WriteBuffer(context, 20, 10);
        });
        @SuppressWarnings("unused")
        WriteBuffer buffer = new WriteBuffer(context, 10, 20);
    }

    @Test
    public void testReachThreshold() throws IOException {
        WriteBuffer buffer = new WriteBuffer(context, 10, 50);
        Assert.assertFalse(buffer.reachThreshold());

        Vertex vertex = context.graphFactory().createVertex(
                        new LongId(1L), new DoubleValue(0.5d));
        // After write, the position is 5
        buffer.writeVertex(vertex);
        Assert.assertFalse(buffer.reachThreshold());

        // After write, the position is 10
        buffer.writeVertex(vertex);
        Assert.assertTrue(buffer.reachThreshold());

        // After write, the position is 15
        buffer.writeVertex(vertex);
        Assert.assertTrue(buffer.reachThreshold());
    }

    @Test
    public void testIsEmpty() throws IOException {
        WriteBuffer buffer = new WriteBuffer(context, 10, 20);
        Assert.assertTrue(buffer.isEmpty());

        Vertex vertex = context.graphFactory().createVertex(
                        new LongId(1L), new DoubleValue(0.5d));
        buffer.writeVertex(vertex);
        Assert.assertFalse(buffer.isEmpty());
    }

    @Test
    public void testClear() throws IOException {
        WriteBuffer buffer = new WriteBuffer(context, 10, 20);
        Assert.assertTrue(buffer.isEmpty());

        Vertex vertex = context.graphFactory().createVertex(
                        new LongId(1L), new DoubleValue(0.5d));
        buffer.writeVertex(vertex);
        Assert.assertFalse(buffer.isEmpty());

        buffer.clear();
        Assert.assertTrue(buffer.isEmpty());
    }

    @Test
    public void testWriteVertex() throws IOException {
        GraphFactory graphFactory = context.graphFactory();

        // NOTE: need ensure the buffer size can hold follow writed bytes
        WriteBuffer buffer = new WriteBuffer(context, 100, 110);
        Vertex vertex = graphFactory.createVertex(new LongId(1L),
                                                  new DoubleValue(0.5d));
        buffer.writeVertex(vertex);
        long position1 = buffer.output().position();
        Assert.assertGt(0L, position1);

        vertex = graphFactory.createVertex(new LongId(1L),
                                           new DoubleValue(0.5d));
        Properties properties = graphFactory.createProperties();
        properties.put("name", new Utf8Id("marko").idValue());
        properties.put("age", new IntValue(18));
        properties.put("city", new ListValue<>(ValueType.ID_VALUE,
                               ImmutableList.of(new Utf8Id("wuhan").idValue(),
                                                new Utf8Id("xian").idValue())));
        vertex.properties(properties);
        buffer.writeVertex(vertex);
        long position2 = buffer.output().position();
        Assert.assertGt(position1, position2);

        vertex = graphFactory.createVertex(new LongId(1L),
                                           new DoubleValue(0.5d));
        vertex.addEdge(graphFactory.createEdge(new LongId(2L)));
        vertex.addEdge(graphFactory.createEdge("knows", new LongId(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", "1111",
                                               new LongId(4L)));
        buffer.writeEdges(vertex);
        long position3 = buffer.output().position();
        Assert.assertGt(position2, position3);
    }

    @Test
    public void testWriteVertexWithEdgeFreq() throws IOException {
        GraphFactory graphFactory = context.graphFactory();

        WriteBuffer buffer = new WriteBuffer(context, 100, 110);
        Vertex vertex = graphFactory.createVertex(new LongId(1L),
                                                  new DoubleValue(0.5d));
        vertex.addEdge(graphFactory.createEdge(new LongId(2L)));
        vertex.addEdge(graphFactory.createEdge("knows", new LongId(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", new LongId(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", "1111",
                                               new LongId(4L)));
        vertex.addEdge(graphFactory.createEdge("watch", "2222",
                                               new LongId(4L)));

        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_NAME, "rank",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.INPUT_EDGE_FREQ, "SINGLE"
        );
        buffer.writeEdges(vertex);
        long position1 = buffer.output().position();
        Assert.assertGt(0L, position1);

        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_NAME, "rank",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.INPUT_EDGE_FREQ, "SINGLE_PER_LABEL"
        );
        // Pass a new context object, because config has updated
        buffer = new WriteBuffer(ComputerContext.instance(), 100, 110);
        buffer.writeEdges(vertex);
        long position2 = buffer.output().position();
        Assert.assertGt(position1, position2);

        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_NAME, "rank",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.INPUT_EDGE_FREQ, "MULTIPLE"
        );
        // Pass a new context object, because config has updated
        buffer = new WriteBuffer(ComputerContext.instance(), 100, 110);
        buffer.writeEdges(vertex);
        long position3 = buffer.output().position();
        Assert.assertGt(position2, position3);
    }
}
