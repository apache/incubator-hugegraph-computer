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

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableSet;

public class MultiQueueTest {

    @Test
    public void testPutAndTake() throws InterruptedException {
        MultiQueue queue = new MultiQueue(2);
        Throwable[] exceptions = new Throwable[3];

        CountDownLatch latch = new CountDownLatch(1);
        Thread thread1 = new Thread(() -> {
            try {
                latch.await();
                queue.put(0, new QueuedMessage(1, 0, MessageType.VERTEX, null));
                queue.put(0, new QueuedMessage(3, 0, MessageType.VERTEX, null));
                queue.put(0, new QueuedMessage(5, 0, MessageType.VERTEX, null));
            } catch (Throwable e) {
                exceptions[0] = e;
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                latch.await();
                queue.put(1, new QueuedMessage(2, 1, MessageType.VERTEX, null));
                queue.put(1, new QueuedMessage(4, 1, MessageType.VERTEX, null));
                queue.put(1, new QueuedMessage(6, 1, MessageType.VERTEX, null));
            } catch (Throwable e) {
                exceptions[1] = e;
            }
        });

        Thread thread3 = new Thread(() -> {
            try {
                latch.countDown();
                Assert.assertTrue(ImmutableSet.of(1, 2).contains(
                                  queue.take().partitionId()));
                Assert.assertTrue(ImmutableSet.of(1, 2).contains(
                                  queue.take().partitionId()));

                Assert.assertTrue(ImmutableSet.of(3, 4).contains(
                                  queue.take().partitionId()));
                Assert.assertTrue(ImmutableSet.of(3, 4).contains(
                                  queue.take().partitionId()));

                Assert.assertTrue(ImmutableSet.of(5, 6).contains(
                                  queue.take().partitionId()));
                Assert.assertTrue(ImmutableSet.of(5, 6).contains(
                                  queue.take().partitionId()));
            } catch (Throwable e) {
                exceptions[2] = e;
            }
        });

        thread1.start();
        thread2.start();
        thread3.start();

        thread1.join();
        thread2.join();
        thread3.join();

        for (Throwable e : exceptions) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testPutAndTakeWithHungry() throws InterruptedException {
        MultiQueue queue = new MultiQueue(2);
        Throwable[] exceptions = new Throwable[3];

        CountDownLatch putLatch = new CountDownLatch(2);
        CountDownLatch takeLatch = new CountDownLatch(1);
        Thread thread1 = new Thread(() -> {
            try {
                queue.put(0, new QueuedMessage(1, 0, MessageType.VERTEX, null));
                putLatch.countDown();

                takeLatch.await();
                queue.put(0, new QueuedMessage(3, 0, MessageType.VERTEX, null));
                queue.put(0, new QueuedMessage(5, 0, MessageType.VERTEX, null));
            } catch (Throwable e) {
                exceptions[0] = e;
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                queue.put(1, new QueuedMessage(2, 1, MessageType.VERTEX, null));
                putLatch.countDown();

                takeLatch.await();
                queue.put(1, new QueuedMessage(4, 1, MessageType.VERTEX, null));
                queue.put(1, new QueuedMessage(6, 1, MessageType.VERTEX, null));
            } catch (Throwable e) {
                exceptions[0] = e;
            }
        });

        Thread thread3 = new Thread(() -> {
            try {
                putLatch.await();
                Assert.assertTrue(ImmutableSet.of(1, 2).contains(
                                  queue.take().partitionId()));
                Assert.assertTrue(ImmutableSet.of(1, 2).contains(
                                  queue.take().partitionId()));

                takeLatch.countDown();
                QueuedMessage message = queue.take();
                Assert.assertTrue(ImmutableSet.of(3, 4).contains(
                                  message.partitionId()));

                // Put the message at the front of the original queue
                queue.putAtFront(message.workerId(), message);

                Assert.assertTrue(ImmutableSet.of(3, 4).contains(
                                  queue.take().partitionId()));
                Assert.assertTrue(ImmutableSet.of(3, 4).contains(
                                  queue.take().partitionId()));

                Assert.assertTrue(ImmutableSet.of(5, 6).contains(
                                  queue.take().partitionId()));
                Assert.assertTrue(ImmutableSet.of(5, 6).contains(
                                  queue.take().partitionId()));
            } catch (Throwable e) {
                exceptions[0] = e;
            }
        });

        thread1.start();
        thread2.start();
        thread3.start();

        thread1.join();
        thread2.join();
        thread3.join();

        for (Throwable e : exceptions) {
            Assert.assertNull(e);
        }
    }
}
