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

package org.apache.hugegraph.computer.core.receiver;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class MessageRecvBuffersTest {

    private static final long WAIT_TIMEOUT = 100L; //ms

    @Test
    public void testBufferToBuffers() {
        long threshold = 1024L;
        int size = 100;
        MessageRecvBuffers buffers = new MessageRecvBuffers(threshold,
                                                            WAIT_TIMEOUT);
        // It's ok to wait for empty buffers
        buffers.waitSorted();

        for (int i = 0; i < 10; i++) {
            addMockBufferToBuffers(buffers, size);
        }

        Assert.assertFalse(buffers.full());
        Assert.assertEquals(1000L, buffers.totalBytes());

        Assert.assertThrows(ComputerException.class, () -> {
            buffers.waitSorted();
        }, e -> {
            Assert.assertContains("Buffers have not been sorted in 100 ms",
                                  e.getMessage());
        });

        addMockBufferToBuffers(buffers, size);
        Assert.assertTrue(buffers.full());

        List<RandomAccessInput> list = buffers.buffers();
        Assert.assertEquals(11, list.size());

        buffers.signalSorted();
        List<RandomAccessInput> list2 = buffers.buffers();
        Assert.assertEquals(11, list2.size());
        Assert.assertEquals(1100L, buffers.totalBytes());

        // It's ok to call waitSorted multi-times
        buffers.waitSorted();
        buffers.waitSorted();

        // Next time again
        buffers.prepareSort();
        List<RandomAccessInput> list3 = buffers.buffers();
        Assert.assertEquals(0, list3.size());
        Assert.assertEquals(0L, buffers.totalBytes());

        buffers.waitSorted();

        for (int i = 0; i < 10; i++) {
            addMockBufferToBuffers(buffers, size);
        }

        Assert.assertEquals(1000L, buffers.totalBytes());
        Assert.assertFalse(buffers.full());

        Assert.assertThrows(ComputerException.class, () -> {
            buffers.waitSorted();
        }, e -> {
            Assert.assertContains("Buffers have not been sorted in 100 ms",
                                  e.getMessage());
        });

        addMockBufferToBuffers(buffers, size);

        Assert.assertTrue(buffers.full());

        List<RandomAccessInput> list4 = buffers.buffers();
        Assert.assertEquals(11, list4.size());
    }

    @Test
    public void testSortBuffer() throws InterruptedException {
        long threshold = 1024L;
        int size = 100;
        MessageRecvBuffers buffers = new MessageRecvBuffers(threshold,
                                                            WAIT_TIMEOUT);
        for (int i = 0; i < 10; i++) {
            addMockBufferToBuffers(buffers, size);
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
        MessageRecvBuffers buffers = new MessageRecvBuffers(threshold,
                                                            WAIT_TIMEOUT);
        for (int i = 0; i < 10; i++) {
            addMockBufferToBuffers(buffers, size);
        }

        Assert.assertThrows(ComputerException.class, () -> {
            buffers.waitSorted();
        }, e -> {
            Assert.assertContains("Buffers have not been sorted in 100 ms",
                                  e.getMessage());
        });
    }

    @Test
    public void testSortInterrupt() throws InterruptedException {
        long threshold = 1024L;
        int size = 100;
        MessageRecvBuffers buffers = new MessageRecvBuffers(threshold,
                                                            WAIT_TIMEOUT);
        for (int i = 0; i < 10; i++) {
            addMockBufferToBuffers(buffers, size);
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

    public static void addMockBufferToBuffers(MessageRecvBuffers buffers,
                                              int mockBufferLength) {
        ReceiverUtil.consumeBuffer(new byte[mockBufferLength],
                                   (NetworkBuffer buffer) -> {
            buffers.addBuffer(buffer);
        });
    }
}
