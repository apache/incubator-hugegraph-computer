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
import com.baidu.hugegraph.testutil.Assert;

public class RecvMessageBuffersTest {

    @Test
    public void testAddBuffer() {
        long threshold = 1024L;
        byte[] bytes = new byte[100];
        long maxWaitTime = 1000L;
        RecvMessageBuffers<byte[]>
                buffers = new RecvMessageBuffers<>(threshold, maxWaitTime);
        for (int i = 0; i < 10; i++) {
            buffers.addBuffer(bytes, bytes.length);
        }
        Assert.assertFalse(buffers.full());
        Assert.assertEquals(1000L, buffers.sumBytes());
        buffers.addBuffer(bytes, bytes.length);
        Assert.assertTrue(buffers.full());

        // Sort buffer
        List<byte[]> list = buffers.list();
        Assert.assertEquals(11, list.size());

        buffers.signalSorted();

        for (int i = 0; i < 10; i++) {
            buffers.addBuffer(bytes, bytes.length);
        }
        Assert.assertFalse(buffers.full());
        buffers.addBuffer(bytes, bytes.length);
        Assert.assertTrue(buffers.full());

        // Sort buffer
        List<byte[]> list2 = buffers.list();
        Assert.assertEquals(11, list2.size());
    }

    @Test
    public void testSortBuffer() throws InterruptedException {
        long threshold = 1024L;
        byte[] bytes = new byte[100];
        long maxWaitTime = 1000L;
        RecvMessageBuffers<byte[]>
                buffers = new RecvMessageBuffers<>(threshold, maxWaitTime);
        for (int i = 0; i < 10; i++) {
            buffers.addBuffer(bytes, bytes.length);
        }
        System.out.println("testSort");
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
    public void testSortTimeout() {
        long threshold = 1024L;
        byte[] bytes = new byte[100];
        long maxWaitTime = 1000L;
        RecvMessageBuffers<byte[]>
                buffers = new RecvMessageBuffers<>(threshold, maxWaitTime);
        for (int i = 0; i < 10; i++) {
            buffers.addBuffer(bytes, bytes.length);
        }

        Assert.assertThrows(ComputerException.class, () -> {
            buffers.waitSorted();
        }, e -> {
            Assert.assertContains("Not sorted in 1000 ms", e.getMessage());
        });
    }

    @Test
    public void testSortInterrupt() throws InterruptedException {
        long threshold = 1024L;
        byte[] bytes = new byte[100];
        long maxWaitTime = 1000L;
        RecvMessageBuffers<byte[]>
                buffers = new RecvMessageBuffers<>(threshold, maxWaitTime);
        for (int i = 0; i < 10; i++) {
            buffers.addBuffer(bytes, bytes.length);
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
