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

package com.baidu.hugegraph.computer.core.receiver;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.testutil.Assert;

public class MessageRecvBuffersTest {

    @Test
    public void testBufferToBuffers() {
        long threshold = 1024L;
        int size = 100;
        long maxWaitTime = 1000L;
        MessageRecvBuffers buffers = new MessageRecvBuffers(threshold,
                                                            maxWaitTime);
        for (int i = 0; i < 10; i++) {
            ReceiverUtil.addMockBufferToBuffers(buffers, size);
        }

        Assert.assertFalse(buffers.full());
        Assert.assertEquals(1000L, buffers.totalBytes());

        ReceiverUtil.addMockBufferToBuffers(buffers, size);
        Assert.assertTrue(buffers.full());

        // Sort buffer
        List<RandomAccessInput> list = buffers.buffers();
        Assert.assertEquals(11, list.size());

        buffers.signalSorted();

        for (int i = 0; i < 10; i++) {
            ReceiverUtil.addMockBufferToBuffers(buffers, size);
        }

        Assert.assertEquals(1000L, buffers.totalBytes());
        Assert.assertFalse(buffers.full());

        ReceiverUtil.addMockBufferToBuffers(buffers, size);

        Assert.assertTrue(buffers.full());

        // Sort buffer
        List<RandomAccessInput> list2 = buffers.buffers();
        Assert.assertEquals(11, list2.size());
    }

    @Test
    public void testSortBuffer() throws InterruptedException {
        long threshold = 1024L;
        int size = 100;
        long maxWaitTime = 1000L;
        MessageRecvBuffers buffers = new MessageRecvBuffers(threshold,
                                                            maxWaitTime);
        for (int i = 0; i < 10; i++) {
            ReceiverUtil.addMockBufferToBuffers(buffers, size);
        }
        CountDownLatch countDownLatch = new CountDownLatch(2);
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.submit(() -> {
            buffers.waitSorted();
            countDownLatch.countDown();
        });

        executorService.submit(() -> {
            buffers.signalSorted();
            countDownLatch.countDown();
        });

        executorService.shutdown();
        countDownLatch.await();
    }

    @Test
    public void testWaitSortTimeout() {
        long threshold = 1024L;
        int size = 100;
        long maxWaitTime = 1000L;
        MessageRecvBuffers buffers = new MessageRecvBuffers(threshold,
                                                            maxWaitTime);
        for (int i = 0; i < 10; i++) {
            ReceiverUtil.addMockBufferToBuffers(buffers, size);
        }

        Assert.assertThrows(ComputerException.class, () -> {
            buffers.waitSorted();
        }, e -> {
            Assert.assertContains("Buffers have not been sorted in 1000 ms",
                                  e.getMessage());
        });
    }

    @Test
    public void testSortInterrupt() throws InterruptedException {
        long threshold = 1024L;
        int size = 100;
        long maxWaitTime = 1000L;
        MessageRecvBuffers buffers = new MessageRecvBuffers(threshold,
                                                            maxWaitTime);
        for (int i = 0; i < 10; i++) {
            ReceiverUtil.addMockBufferToBuffers(buffers, size);
        }
        AtomicBoolean success = new AtomicBoolean(false);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread sortThread = new Thread(() -> {
            buffers.waitSorted();
        });
        sortThread.setUncaughtExceptionHandler((t, e) -> {
            String expected = "Interrupted while waiting buffers to be sorted";
            try {
                Assert.assertContains(expected, e.getMessage());
                success.set(true);
            } finally {
                countDownLatch.countDown();
            }
        });
        sortThread.start();
        sortThread.interrupt();
        countDownLatch.await();
        Assert.assertTrue(success.get());
    }
}
