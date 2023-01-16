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
package org.apache.hugegraph.computer.dist;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Log;
import org.junit.Test;
import org.slf4j.Logger;

public class HugeGraphComputerTest extends UnitTestBase {

    private static final Logger LOG = Log.logger(HugeGraphComputerTest.class);

    @Test
    public void testServiceWith1Worker() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Throwable[] exceptions = new Throwable[2];
        String masterConfPath = HugeGraphComputerTest.class.getResource(
                                "/computer.properties").getPath();
        String work1ConfPath = HugeGraphComputerTest.class.getResource(
                               "/computer.properties").getPath();
        pool.submit(() -> {
            try {
                Thread.sleep(2000L);
                String[] args = {work1ConfPath, "worker", "local"};
                HugeGraphComputer.main(args);
            } catch (Throwable e) {
                LOG.error("Failed to start worker", e);
                exceptions[0] = e;
            } finally {
                countDownLatch.countDown();
            }
        });

        pool.submit(() -> {
            try {
                String[] args = {masterConfPath, "master", "local"};
                HugeGraphComputer.main(args);
            } catch (Throwable e) {
                LOG.error("Failed to start master", e);
                exceptions[1] = e;
            } finally {
                countDownLatch.countDown();
            }
        });

        countDownLatch.await();
        pool.shutdownNow();

        Assert.assertFalse(Arrays.asList(exceptions).toString(),
                           this.existError(exceptions));
    }

    @Test
    public void testServiceWithError() {
        String work1ConfPath = HugeGraphComputerTest.class.getResource(
                               "/computer.properties").getPath();
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> {
                                String[] args1 = {work1ConfPath, "worker111",
                                                  "local"};
                                HugeGraphComputer.main(args1);
                            });
    }

    @Test
    public void testPrintUncaughtException() throws InterruptedException {
        AtomicBoolean isRun = new AtomicBoolean(false);
        Thread.UncaughtExceptionHandler handler = (t, e) -> {
            isRun.compareAndSet(false, true);
        };
        Thread.setDefaultUncaughtExceptionHandler(handler);
        HugeGraphComputer.setUncaughtExceptionHandler();
        Thread t = new Thread(() -> {
            throw new RuntimeException();
        });
        t.start();
        t.join();
        Assert.assertTrue(isRun.get());
    }
}
