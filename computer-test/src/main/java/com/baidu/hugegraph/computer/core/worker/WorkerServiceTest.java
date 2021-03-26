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

package com.baidu.hugegraph.computer.core.worker;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.master.MasterService;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Log;

public class WorkerServiceTest {
    private static final Logger LOG = Log.logger(WorkerServiceTest.class);

    private MasterService masterService;
    private WorkerService workerService;

    @Before
    public void setup() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.BSP_LOG_INTERVAL, "30000",
                ComputerOptions.BSP_MAX_SUPER_STEP, "2"
        );
        Config config = ComputerContext.instance().config();
        this.masterService = new MasterService(config);
        this.workerService = new MockWorkerService(config);
    }

    @After
    public void close() {
        this.workerService.close();
        this.masterService.close();
    }

    @Test
    public void testService() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        pool.submit(() -> {
            try  {
                this.workerService.init();
                this.workerService.execute();
            } finally {
                countDownLatch.countDown();
            }
        });
        pool.submit(() -> {
            try {
                this.masterService.init();
                this.masterService.execute();
            } finally {
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        pool.shutdownNow();

        Assert.assertEquals(100L,
                            this.masterService.totalVertexCount());
        Assert.assertEquals(200L,
                            this.masterService.totalEdgeCount());
        Assert.assertEquals(50L,
                            this.masterService.finishedVertexCount());
        Assert.assertEquals(60L,
                            this.masterService.messageCount());
        Assert.assertEquals(70L,
                            this.masterService.messageBytes());

        Assert.assertEquals(100L,
                            this.workerService.totalVertexCount());
        Assert.assertEquals(200L,
                            this.workerService.totalEdgeCount());
    }
}
