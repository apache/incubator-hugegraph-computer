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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.graph.value.ListValue;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.StreamGraphInput;
import org.apache.hugegraph.computer.core.store.entry.EntryInput;
import org.apache.hugegraph.computer.core.store.entry.EntryInputImpl;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class WriteBuffersTest extends UnitTestBase {

    @Test
    public void testConstructor() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new WriteBuffers(context(), 0, 20);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains(
                              "The threshold of buffer must be > 0"));
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new WriteBuffers(context(), 10, -1);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains(
                              "The capacity of buffer must be > 0"));
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new WriteBuffers(context(), 20, 10);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains(
                              "The threshold must be <= capacity"));
        });
        @SuppressWarnings("unused")
        WriteBuffers buffers = new WriteBuffers(context(), 10, 20);
    }

    @Test
    public void testReachThreshold() throws IOException {
        WriteBuffers buffers = new WriteBuffers(context(), 20, 50);
        Assert.assertFalse(buffers.reachThreshold());

        Vertex vertex = context().graphFactory().createVertex(
                BytesId.of(1L), new DoubleValue(0.5d));
        // After write, the position is 11
        buffers.writeVertex(vertex);
        Assert.assertFalse(buffers.reachThreshold());

        // After write, the position is 22
        buffers.writeVertex(vertex);
        Assert.assertTrue(buffers.reachThreshold());

        // After write, the position is 33
        buffers.writeVertex(vertex);
        Assert.assertTrue(buffers.reachThreshold());
    }

    @Test
    public void testIsEmpty() throws IOException {
        WriteBuffers buffers = new WriteBuffers(context(), 10, 20);
        Assert.assertTrue(buffers.isEmpty());

        Vertex vertex = context().graphFactory().createVertex(
                        BytesId.of(1L), new DoubleValue(0.5d));
        buffers.writeVertex(vertex);
        Assert.assertFalse(buffers.isEmpty());
    }

    @Test
    public void testWriteVertex() throws IOException {
        GraphFactory graphFactory = context().graphFactory();

        // NOTE: need ensure the buffer size can hold follow writed bytes
        WriteBuffers buffers = new WriteBuffers(context(), 100, 110);
        Vertex vertex = graphFactory.createVertex(BytesId.of(1L),
                                                  new DoubleValue(0.5d));
        buffers.writeVertex(vertex);
        WriteBuffer buffer = Whitebox.getInternalState(buffers,
                                                       "writingBuffer");
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
        buffers.writeVertex(vertex);
        buffer = Whitebox.getInternalState(buffers, "writingBuffer");
        long position2 = buffer.output().position();
        Assert.assertGt(position1, position2);

        vertex = graphFactory.createVertex(BytesId.of(1L),
                                           new DoubleValue(0.5d));
        vertex.addEdge(graphFactory.createEdge(BytesId.of(2L)));
        vertex.addEdge(graphFactory.createEdge("knows", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", "1111",
                                               BytesId.of(4L)));
        buffers.writeEdges(vertex);
        buffer = Whitebox.getInternalState(buffers, "writingBuffer");
        long position3 = buffer.output().position();
        Assert.assertGt(position2, position3);
    }

    @Test
    public void testWriteMessage() throws IOException {
        WriteBuffers buffers = new WriteBuffers(context(), 50, 100);
        WriteBuffer buffer = Whitebox.getInternalState(buffers,
                                                       "writingBuffer");

        buffers.writeMessage(BytesId.of(1L), new DoubleValue(0.85D));
        long position1 = buffer.output().position();
        Assert.assertGt(0L, position1);

        buffers.writeMessage(BytesId.of(2L), new DoubleValue(0.15D));
        long position2 = buffer.output().position();
        Assert.assertGt(position1, position2);
    }

    @Test
    public void testPrepareSorting() throws IOException, InterruptedException {
        GraphFactory graphFactory = context().graphFactory();

        WriteBuffers buffers = new WriteBuffers(context(), 50, 100);
        Vertex vertex = graphFactory.createVertex(BytesId.of(1L),
                                                  new DoubleValue(0.5d));
        vertex.addEdge(graphFactory.createEdge(BytesId.of(2L)));
        vertex.addEdge(graphFactory.createEdge("knows", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", "1111",
                                               BytesId.of(4L)));
        buffers.writeEdges(vertex);
        // Reached threshold, the position is 76
        Assert.assertTrue(buffers.reachThreshold());
        Assert.assertFalse(buffers.isEmpty());
        // Exchange writing buffer and sorting buffer
        buffers.prepareSorting();
        Assert.assertFalse(buffers.reachThreshold());
        Assert.assertTrue(buffers.isEmpty());

        Thread thread1 = new Thread(() -> {
            Assert.assertThrows(ComputerException.class, () -> {
                buffers.prepareSorting();
            }, e -> {
                Assert.assertTrue(e.getMessage().contains("Interrupted"));
            });
        });
        thread1.start();

        Thread.sleep(100);
        thread1.interrupt();
    }

    @Test
    public void testSwitchAndFinishSorting() throws IOException,
                                                    InterruptedException {
        GraphFactory graphFactory = context().graphFactory();

        WriteBuffers buffers = new WriteBuffers(context(), 50, 100);
        Vertex vertex = graphFactory.createVertex(BytesId.of(1L),
                                                  new DoubleValue(0.5d));
        vertex.addEdge(graphFactory.createEdge(BytesId.of(2L)));
        vertex.addEdge(graphFactory.createEdge("knows", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", "1111",
                                               BytesId.of(4L)));
        buffers.writeEdges(vertex);
        // Reached threshold, the position is 76
        Assert.assertTrue(buffers.reachThreshold());
        /*
         * When reached threshold, switchForSorting will exchange writing buffer
         * and sorting buffer, so the writing buffer become clean
         */
        buffers.switchForSorting();
        Assert.assertFalse(buffers.reachThreshold());
        Assert.assertTrue(buffers.isEmpty());
        // Nothing changed
        buffers.switchForSorting();
        Assert.assertFalse(buffers.reachThreshold());
        Assert.assertTrue(buffers.isEmpty());
        // The writing buffer reached threshold again, position is 76
        buffers.writeEdges(vertex);

        AtomicInteger counter = new AtomicInteger(0);
        Thread thread1 = new Thread(() -> {
            // Await until finishSorting method called
            buffers.switchForSorting();
            Assert.assertEquals(2, counter.get());
        });
        Thread thread2 = new Thread(() -> {
            while (counter.get() < 2) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Assert.fail(e.getMessage());
                }
                counter.incrementAndGet();
            }
            // counter is 2
            buffers.finishSorting();
        });
        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();
    }

    @Test
    public void wrapForRead() throws IOException {
        GraphFactory graphFactory = context().graphFactory();

        WriteBuffers buffers = new WriteBuffers(context(), 10, 20);
        Vertex vertex = graphFactory.createVertex(BytesId.of(1L),
                                                  new DoubleValue(0.5d));
        buffers.writeVertex(vertex);
        buffers.prepareSorting();

        try (RandomAccessInput input = buffers.wrapForRead()) {
            EntryInput entryInput = new EntryInputImpl(input);
            StreamGraphInput graphInput = new StreamGraphInput(context(),
                                                               entryInput);
            vertex.value(null);
            Assert.assertEquals(vertex, graphInput.readVertex());
        }
    }
}
