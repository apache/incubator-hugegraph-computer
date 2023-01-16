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

import java.util.concurrent.CountDownLatch;

import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class MultiQueueTest {

    @Test
    public void testPutAndTake() throws InterruptedException {
        MultiQueue queue = new MultiQueue(2);
        Throwable[] exceptions = new Throwable[3];

        CountDownLatch[] latches = new CountDownLatch[3];
        for (int i = 0; i < latches.length; i++) {
            latches[i] = new CountDownLatch(1);
        }

        Thread thread1 = new Thread(() -> {
            try {
                latches[0].await();
                queue.put(0, new QueuedMessage(1, MessageType.VERTEX, null));
                latches[1].await();
                queue.put(0, new QueuedMessage(3, MessageType.VERTEX, null));
                latches[2].await();
                queue.put(0, new QueuedMessage(5, MessageType.VERTEX, null));
            } catch (Throwable e) {
                exceptions[0] = e;
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                latches[0].await();
                queue.put(1, new QueuedMessage(2, MessageType.VERTEX, null));
                latches[1].await();
                queue.put(1, new QueuedMessage(4, MessageType.VERTEX, null));
                latches[2].await();
                queue.put(1, new QueuedMessage(6, MessageType.VERTEX, null));
            } catch (Throwable e) {
                exceptions[1] = e;
            }
        });

        Thread thread3 = new Thread(() -> {
            try {
                latches[0].countDown();
                Assert.assertTrue(ImmutableSet.of(1, 2).contains(
                                  queue.take().partitionId()));
                Assert.assertTrue(ImmutableSet.of(1, 2).contains(
                                  queue.take().partitionId()));

                latches[1].countDown();
                Assert.assertTrue(ImmutableSet.of(3, 4).contains(
                                  queue.take().partitionId()));
                Assert.assertTrue(ImmutableSet.of(3, 4).contains(
                                  queue.take().partitionId()));

                latches[2].countDown();
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
    public void testPutAndTakeWithPutAtFront() throws InterruptedException {
        MultiQueue queue = new MultiQueue(2);
        Throwable[] exceptions = new Throwable[3];

        CountDownLatch[] latches = new CountDownLatch[3];
        for (int i = 0; i < latches.length; i++) {
            latches[i] = new CountDownLatch(1);
        }

        Thread thread1 = new Thread(() -> {
            try {
                latches[0].await();
                queue.put(0, new QueuedMessage(1, MessageType.VERTEX, null));

                latches[1].await();
                queue.put(0, new QueuedMessage(3, MessageType.VERTEX, null));
                latches[2].await();
                queue.put(0, new QueuedMessage(5, MessageType.VERTEX, null));
            } catch (Throwable e) {
                exceptions[0] = e;
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                latches[0].await();
                queue.put(1, new QueuedMessage(2, MessageType.VERTEX, null));

                latches[1].await();
                queue.put(1, new QueuedMessage(4, MessageType.VERTEX, null));
                latches[2].await();
                queue.put(1, new QueuedMessage(6, MessageType.VERTEX, null));
            } catch (Throwable e) {
                exceptions[1] = e;
            }
        });

        Thread thread3 = new Thread(() -> {
            try {
                latches[0].countDown();
                QueuedMessage message1 = queue.take();
                QueuedMessage message2 = queue.take();
                Assert.assertTrue(ImmutableSet.of(1, 2).contains(
                                  message1.partitionId()));
                Assert.assertTrue(ImmutableSet.of(1, 2).contains(
                                  message2.partitionId()));

                latches[1].countDown();
                QueuedMessage message = queue.take();
                Assert.assertTrue(ImmutableSet.of(3, 4).contains(
                                  message.partitionId()));

                // Put the message at the front of the original queue
                if ((message.partitionId() & 0x01) == 1) {
                    queue.putAtFront(0, message);
                } else {
                    queue.putAtFront(1, message);
                }

                Assert.assertTrue(ImmutableSet.of(3, 4).contains(
                                  queue.take().partitionId()));
                Assert.assertTrue(ImmutableSet.of(3, 4).contains(
                                  queue.take().partitionId()));

                latches[2].countDown();
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
    public void testTakeWithWait() throws InterruptedException {
        MultiQueue queue = new MultiQueue(2);
        Throwable[] exceptions = new Throwable[3];

        Thread thread1 = new Thread(() -> {
            try {
                Thread.sleep(100);
                queue.put(0, new QueuedMessage(1, MessageType.VERTEX, null));
                Thread.sleep(200);
                queue.put(0, new QueuedMessage(3, MessageType.VERTEX, null));
                Thread.sleep(300);
                queue.put(0, new QueuedMessage(5, MessageType.VERTEX, null));
            } catch (Throwable e) {
                exceptions[0] = e;
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                Thread.sleep(100);
                queue.put(1, new QueuedMessage(2, MessageType.VERTEX, null));
                Thread.sleep(200);
                queue.put(1, new QueuedMessage(4, MessageType.VERTEX, null));
                Thread.sleep(300);
                queue.put(1, new QueuedMessage(6, MessageType.VERTEX, null));
            } catch (Throwable e) {
                exceptions[1] = e;
            }
        });

        Thread thread3 = new Thread(() -> {
            try {
                queue.take();
                queue.take();
                queue.take();
                queue.take();
                queue.take();
                queue.take();
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
}
