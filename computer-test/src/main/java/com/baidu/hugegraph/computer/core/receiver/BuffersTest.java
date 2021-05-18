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

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

public class BuffersTest {

    @Test
    public void test() throws InterruptedException {
        long threshold = 1024L;
        byte[] bytes = new byte[100];
        Buffers<byte[]> buffers = new Buffers<>(threshold);
        for (int i = 0; i < 10; i++) {
            buffers.addBuffer(bytes, bytes.length);
        }
        Assert.assertFalse(buffers.isFull());
        Assert.assertEquals(1000L, buffers.sumBytes());
        buffers.addBuffer(bytes, bytes.length);
        Assert.assertTrue(buffers.isFull());

        // Sort buffer
        List<byte[]> list = buffers.list();
        Assert.assertEquals(11, list.size());

        buffers.signalSorted();

        for (int i = 0; i < 10; i++) {
            buffers.addBuffer(bytes, bytes.length);
        }
        Assert.assertFalse(buffers.isFull());
        buffers.addBuffer(bytes, bytes.length);
        Assert.assertTrue(buffers.isFull());

        // Sort buffer
        List<byte[]> list2 = buffers.list();
        Assert.assertEquals(11, list2.size());

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
}
