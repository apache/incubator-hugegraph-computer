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

package com.baidu.hugegraph.computer.core.allocator;

import static org.junit.Assert.assertNotSame;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Random;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

import io.netty.util.Recycler;

public class RecyclersTest {

    private static final Method threadLocalCapacityMethod;

    static {
        try {
            Method method = Recycler.class
                                    .getDeclaredMethod("threadLocalCapacity");
            method.setAccessible(true);
            threadLocalCapacityMethod = method;
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("No method 'threadLocalCapacity'");
        }
    }

    private static Recycler<RecyclableObject> newRecycler(final int max) {
        return new Recycler<RecyclableObject>(max) {
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
                throws InterruptedException {
        Recycler<RecyclableObject> recycler = newRecycler(512);
        RecyclableObject object = recycler.get();
        Thread thread1 = new Thread(() -> object.handle.recycle(object));
        thread1.start();
        thread1.join();
        Assert.assertSame(object, recycler.get());
    }

    @Test
    public void testRecycleMoreThanOnceAtDifferentThread()
                throws InterruptedException {
        Recycler<RecyclableObject> recyclers = newRecycler(1024);
        RecyclableObject object = recyclers.get();

        Thread thread1 = new Thread(() -> object.handle.recycle(object));
        thread1.start();
        thread1.join();

        Thread thread2 = new Thread(() -> {
            Assert.assertThrows(IllegalStateException.class, () -> {
                object.handle.recycle(object);
            }, e -> {
                Assert.assertTrue(e.getMessage().contains("recycled already"));
            });
        });
        thread2.start();
        thread2.join();
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
        Recycler<RecyclableObject> recycler = newRecycler(maxCapacity);
        RecyclableObject[] objects = new RecyclableObject[maxCapacity * 3];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = recycler.get();
        }

        for (int i = 0; i < objects.length; i++) {
            objects[i].handle.recycle(objects[i]);
            objects[i] = null;
        }

        int threadLocalCapacity = (Integer) threadLocalCapacityMethod.invoke(
                                            recycler);
        Assert.assertTrue(maxCapacity >= threadLocalCapacity);
    }

    private static final class RecyclableObject {

        private final Recycler.Handle<RecyclableObject> handle;

        private RecyclableObject(Recycler.Handle<RecyclableObject> handle) {
            this.handle = handle;
        }
    }
}
