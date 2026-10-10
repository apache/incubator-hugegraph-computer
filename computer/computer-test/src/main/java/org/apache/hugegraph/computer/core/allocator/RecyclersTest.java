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

import static org.junit.Assert.assertNotSame;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import io.netty.util.Recycler;

public class RecyclersTest {

    private static Recycler<RecyclableObject> newRecycler(final int max) {
        // Retain every handle so capacity assertions do not depend on sampling.
        return new Recycler<RecyclableObject>(max, 1, 32) {
            @Override
            protected RecyclableObject newObject(
                      Recycler.Handle<RecyclableObject> handle) {
                return new RecyclableObject(handle);
            }
        };
    }

    @Test
    public void testRecycle() {
        Recycler<RecyclableObject> recycler = newRecycler(16);
        RecyclableObject object1 = recycler.get();
        object1.handle.recycle(object1);

        RecyclableObject object2 = recycler.get();
        Assert.assertSame(object1, object2);
        object2.handle.recycle(object2);
    }

    @Test
    public void testMultiRecycle() {
        Recycler<RecyclableObject> recycler = newRecycler(16);
        RecyclableObject object = recycler.get();
        object.handle.recycle(object);
        Assert.assertThrows(IllegalStateException.class, () -> {
            object.handle.recycle(object);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("recycled already"));
        });
    }

    @Test
    public void testMultiRecycleAtDifferentThread()
                throws InterruptedException, ExecutionException {
        Recycler<RecyclableObject> recycler = newRecycler(512);
        RecyclableObject object = recycler.get();
        runInAnotherThread(() -> object.handle.recycle(object));
        Assert.assertSame(object, recycler.get());
    }

    @Test
    public void testRecycleMoreThanOnceAtDifferentThread()
                throws InterruptedException, ExecutionException {
        Recycler<RecyclableObject> recyclers = newRecycler(1024);
        RecyclableObject object = recyclers.get();

        runInAnotherThread(() -> object.handle.recycle(object));
        runInAnotherThread(() -> {
            Assert.assertThrows(IllegalStateException.class, () -> {
                object.handle.recycle(object);
            }, e -> {
                Assert.assertTrue(e.getMessage().contains("recycled already"));
            });
        });
    }

    @Test
    public void testRecycleDisable() {
        Recycler<RecyclableObject> recycler = newRecycler(-1);
        RecyclableObject object1 = recycler.get();
        object1.handle.recycle(object1);

        RecyclableObject object2 = recycler.get();
        assertNotSame(object1, object2);
        object2.handle.recycle(object2);
    }

    @Test
    public void testMaxCapacity() throws InterruptedException, ExecutionException {
        // Netty rounds queue capacities to powers of two.
        for (int capacity : new int[] {256, 512, 1024}) {
            testMaxCapacity(capacity);
        }
    }

    private void testMaxCapacity(final int maxCapacity)
            throws InterruptedException, ExecutionException {
        Recycler<RecyclableObject> recycler = newRecycler(maxCapacity);
        RecyclableObject[] objects = new RecyclableObject[maxCapacity * 3];
        Set<RecyclableObject> allocated = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int i = 0; i < objects.length; i++) {
            objects[i] = recycler.get();
            Assert.assertTrue(allocated.add(objects[i]));
        }

        // Return from another thread to fill the bounded queue, not a local batch.
        runInAnotherThread(() -> {
            for (RecyclableObject object : objects) {
                object.handle.recycle(object);
            }
        });

        int reused = 0;
        Set<RecyclableObject> acquired = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int i = 0; i < objects.length; i++) {
            RecyclableObject object = recycler.get();
            Assert.assertTrue(acquired.add(object));
            if (allocated.contains(object)) {
                reused++;
            }
        }
        Assert.assertEquals(maxCapacity, reused);
    }

    private static void runInAnotherThread(Runnable action)
            throws InterruptedException, ExecutionException {
        FutureTask<Void> task = new FutureTask<>(action, null);
        Thread thread = new Thread(task);
        thread.start();
        thread.join();
        task.get();
    }

    private static final class RecyclableObject {

        private final Recycler.Handle<RecyclableObject> handle;

        private RecyclableObject(Recycler.Handle<RecyclableObject> handle) {
            this.handle = handle;
        }
    }
}
