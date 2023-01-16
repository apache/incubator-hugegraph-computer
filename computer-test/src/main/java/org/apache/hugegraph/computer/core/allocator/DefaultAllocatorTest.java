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

package org.apache.hugegraph.computer.core.allocator;

import java.lang.reflect.InvocationTargetException;
import java.util.Random;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class DefaultAllocatorTest extends UnitTestBase {

    @Test
    public void testVertexRecycle() {
        Allocator allocator = context().allocator();
        RecyclerReference<Vertex> reference1 = allocator.newVertex();
        allocator.freeVertex(reference1);

        RecyclerReference<Vertex> reference2 = allocator.newVertex();
        Assert.assertSame(reference1, reference2);
        Assert.assertSame(reference1.get(), reference2.get());

        RecyclerReference<Vertex> reference3 = allocator.newVertex();
        Assert.assertNotSame(reference1, reference3);
        Assert.assertNotSame(reference1.get(), reference3.get());

        allocator.freeVertex(reference2);
        allocator.freeVertex(reference3);
    }

    @Test
    public void testEdgeRecycle() {
        Allocator allocator = context().allocator();
        RecyclerReference<Edge> reference1 = allocator.newEdge();
        allocator.freeEdge(reference1);

        RecyclerReference<Edge> reference2 = allocator.newEdge();
        Assert.assertSame(reference1, reference2);
        Assert.assertSame(reference1.get(), reference2.get());

        RecyclerReference<Edge> reference3 = allocator.newEdge();
        Assert.assertNotSame(reference1, reference3);
        Assert.assertNotSame(reference1.get(), reference3.get());

        allocator.freeEdge(reference2);
        allocator.freeEdge(reference3);
    }

    @Test
    public void testMultiRecycle() {
        Allocator allocator = context().allocator();
        RecyclerReference<Vertex> reference1 = allocator.newVertex();
        allocator.freeVertex(reference1);

        Assert.assertThrows(IllegalStateException.class, () -> {
            allocator.freeVertex(reference1);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("recycled already"));
        });
    }

    @Test
    public void testMultiRecycleAtDifferentThread()
                throws InterruptedException {
        Allocator allocator = context().allocator();
        RecyclerReference<Vertex> reference1 = allocator.newVertex();
        Thread thread1 = new Thread(() -> allocator.freeVertex(reference1));
        thread1.start();
        thread1.join();

        RecyclerReference<Vertex> reference2 = allocator.newVertex();
        Assert.assertSame(reference1, reference2);
        Assert.assertSame(reference1.get(), reference2.get());
        allocator.freeVertex(reference2);
    }

    @Test
    public void testRecycleMoreThanOnceAtDifferentThread()
                throws InterruptedException {
        Allocator allocator = context().allocator();
        RecyclerReference<Vertex> reference1 = allocator.newVertex();

        Thread thread1 = new Thread(() -> allocator.freeVertex(reference1));
        thread1.start();
        thread1.join();

        Thread thread2 = new Thread(() -> {
            Assert.assertThrows(IllegalStateException.class, () -> {
                allocator.freeVertex(reference1);
            }, e -> {
                Assert.assertTrue(e.getMessage().contains("recycled already"));
            });
        });
        thread2.start();
        thread2.join();
    }

    @Test
    public void testAutoRecycle() {
        Allocator allocator = context().allocator();
        RecyclerReference<Vertex> reference;
        try (RecyclerReference<Vertex> reference1 = allocator.newVertex();
             RecyclerReference<Vertex> reference2 = allocator.newVertex()) {
            reference = reference1;
            Assert.assertNotSame(reference1, reference2);
            Assert.assertNotSame(reference1.get(), reference2.get());
        }
        Assert.assertThrows(IllegalStateException.class, () -> {
            allocator.freeVertex(reference);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("recycled already"));
        });
    }

    @Test
    public void testMaxCapacity() throws InvocationTargetException,
                                         IllegalAccessException {
        testMaxCapacity(300);
        Random rand = new Random();
        for (int i = 0; i < 50; i++) {
            testMaxCapacity(rand.nextInt(1000) + 256); // 256 - 1256
        }
    }

    private static void testMaxCapacity(final int maxCapacity)
            throws InvocationTargetException, IllegalAccessException {
        String capacityValue = String.valueOf(maxCapacity);
        UnitTestBase.updateOptions(
            ComputerOptions.ALLOCATOR_MAX_VERTICES_PER_THREAD, capacityValue
        );
        Allocator allocator = context().allocator();
        @SuppressWarnings("unchecked")
        RecyclerReference<Vertex>[] references =
                                    new RecyclerReference[maxCapacity * 3];
        for (int i = 0; i < references.length; i++) {
            references[i] = allocator.newVertex();
        }

        for (int i = 0; i < references.length; i++) {
            allocator.freeVertex(references[i]);
            references[i] = null;
        }

        // TODO: Assert something
    }
}
