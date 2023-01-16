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

package org.apache.hugegraph.computer.core.sender;

import java.io.IOException;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.graph.value.ListValue;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

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
        WriteBuffer buffer = new WriteBuffer(context, 20, 50);
        Assert.assertFalse(buffer.reachThreshold());

        Vertex vertex = context.graphFactory().createVertex(
                BytesId.of(1L), new DoubleValue(0.5d));
        // After write, the position is 11
        buffer.writeVertex(vertex);
        Assert.assertFalse(buffer.reachThreshold());

        // After write, the position is 22
        buffer.writeVertex(vertex);
        Assert.assertTrue(buffer.reachThreshold());

        // After write, the position is 33
        buffer.writeVertex(vertex);
        Assert.assertTrue(buffer.reachThreshold());
    }

    @Test
    public void testIsEmpty() throws IOException {
        WriteBuffer buffer = new WriteBuffer(context, 10, 20);
        Assert.assertTrue(buffer.isEmpty());

        Vertex vertex = context.graphFactory().createVertex(
                        BytesId.of(1L), new DoubleValue(0.5d));
        buffer.writeVertex(vertex);
        Assert.assertFalse(buffer.isEmpty());
    }

    @Test
    public void testClear() throws IOException {
        WriteBuffer buffer = new WriteBuffer(context, 10, 20);
        Assert.assertTrue(buffer.isEmpty());

        Vertex vertex = context.graphFactory().createVertex(
                        BytesId.of(1L), new DoubleValue(0.5d));
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
        Vertex vertex = graphFactory.createVertex(BytesId.of(1L),
                                                  new DoubleValue(0.5d));
        buffer.writeVertex(vertex);
        long position1 = buffer.output().position();
        Assert.assertGt(0L, position1);

        vertex = graphFactory.createVertex(BytesId.of(1L),
                                           new DoubleValue(0.5d));
        Properties properties = graphFactory.createProperties();
        properties.put("name", BytesId.of("marko"));
        properties.put("age", new IntValue(18));
        properties.put("city", new ListValue<>(ValueType.ID,
                                               ImmutableList.of(BytesId.of("wuhan"),
                                                BytesId.of("xian"))));
        vertex.properties(properties);
        buffer.writeVertex(vertex);
        long position2 = buffer.output().position();
        Assert.assertGt(position1, position2);

        vertex = graphFactory.createVertex(BytesId.of(1L),
                                           new DoubleValue(0.5d));
        vertex.addEdge(graphFactory.createEdge(BytesId.of(2L)));
        vertex.addEdge(graphFactory.createEdge("knows", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", "1111",
                                               BytesId.of(4L)));
        buffer.writeEdges(vertex);
        long position3 = buffer.output().position();
        Assert.assertGt(position2, position3);
    }

    @Test
    public void testWriteVertexWithEdgeFreq() throws IOException {
        GraphFactory graphFactory = context.graphFactory();

        Vertex vertex = graphFactory.createVertex(BytesId.of(1L),
                                                  new DoubleValue(0.5d));
        vertex.addEdge(graphFactory.createEdge(BytesId.of(2L)));
        vertex.addEdge(graphFactory.createEdge("knows", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", "1111",
                                               BytesId.of(4L)));
        vertex.addEdge(graphFactory.createEdge("watch", "2222",
                                               BytesId.of(4L)));

        WriteBuffer buffer;
        UnitTestBase.updateOptions(
                ComputerOptions.INPUT_EDGE_FREQ, "SINGLE"
        );
        buffer = new WriteBuffer(ComputerContext.instance(), 100, 110);
        buffer.writeEdges(vertex);
        long position1 = buffer.output().position();
        Assert.assertGt(0L, position1);

        UnitTestBase.updateOptions(
            ComputerOptions.INPUT_EDGE_FREQ, "SINGLE_PER_LABEL"
        );
        // Pass a new context object, because config has updated
        buffer = new WriteBuffer(ComputerContext.instance(), 100, 110);
        buffer.writeEdges(vertex);
        long position2 = buffer.output().position();
        Assert.assertGte(position1, position2);

        UnitTestBase.updateOptions(
            ComputerOptions.INPUT_EDGE_FREQ, "MULTIPLE"
        );
        // Pass a new context object, because config has updated
        buffer = new WriteBuffer(ComputerContext.instance(), 100, 110);
        buffer.writeEdges(vertex);
        long position3 = buffer.output().position();
        Assert.assertGte(position2, position3);
    }

    @Test
    public void testWriteMessage() throws IOException {
        WriteBuffer buffer = new WriteBuffer(context, 50, 100);

        buffer.writeMessage(BytesId.of(1L), new DoubleValue(0.85D));
        long position1 = buffer.output().position();
        Assert.assertGt(0L, position1);

        buffer.writeMessage(BytesId.of(2L), new DoubleValue(0.15D));
        long position2 = buffer.output().position();
        Assert.assertGt(position1, position2);
    }
}
