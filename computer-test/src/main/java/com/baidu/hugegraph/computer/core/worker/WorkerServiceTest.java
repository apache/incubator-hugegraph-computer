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

import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.master.MasterService;
import com.baidu.hugegraph.config.RpcOptions;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Log;

public class WorkerServiceTest {

    private static final Logger LOG = Log.logger(WorkerServiceTest.class);

    @Test
    public void testServiceSuccess() throws InterruptedException {
        MasterService masterService = new MasterService();
        WorkerService workerService = new MockWorkerService();
        try {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch countDownLatch = new CountDownLatch(2);
            Throwable[] exceptions = new Throwable[2];

            pool.submit(() -> {
                UnitTestBase.updateWithRequiredOptions(
                        RpcOptions.RPC_REMOTE_URL, "127.0.0.1:8090",
                        ComputerOptions.JOB_ID, "local_001",
                        ComputerOptions.JOB_WORKERS_COUNT, "1",
                        ComputerOptions.BSP_LOG_INTERVAL, "30000",
                        ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                        ComputerOptions.WORKER_COMPUTATION_CLASS,
                        MockComputation.class.getName(),
                        ComputerOptions.MASTER_COMPUTATION_CLASS,
                        MockMasterComputation.class.getName()
                );
                Config config = ComputerContext.instance().config();
                try {
                    workerService.init(config);
                    workerService.execute();
                } catch (Throwable e) {
                    LOG.error("Failed to start worker", e);
                    exceptions[0] = e;
                } finally {
                    countDownLatch.countDown();
                }
            });

            pool.submit(() -> {
                UnitTestBase.updateWithRequiredOptions(
                        RpcOptions.RPC_SERVER_HOST, "127.0.0.1",
                        ComputerOptions.JOB_ID, "local_001",
                        ComputerOptions.JOB_WORKERS_COUNT, "1",
                        ComputerOptions.BSP_LOG_INTERVAL, "30000",
                        ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                        ComputerOptions.WORKER_COMPUTATION_CLASS,
                        MockComputation.class.getName(),
                        ComputerOptions.MASTER_COMPUTATION_CLASS,
                        MockMasterComputation.class.getName()
                );
                Config config = ComputerContext.instance().config();
                try {
                    masterService.init(config);
                    masterService.execute();
                } catch (Throwable e) {
                    LOG.error("Failed to start master", e);
                    exceptions[1] = e;
                } finally {
                    countDownLatch.countDown();
                }
            });

            countDownLatch.await();
            pool.shutdownNow();

            Assert.assertFalse(existError(exceptions));
        } finally {
            workerService.close();
            masterService.close();
        }
    }

    private boolean existError(Throwable[] exceptions) {
        boolean error = false;
        for (Throwable e : exceptions) {
            if (e != null) {
                error = true;
            }
        }
        return error;
    }

    @Test
    public void testFailToConnectEtcd() {
        WorkerService workerService = new MockWorkerService();
        UnitTestBase.updateWithRequiredOptions(
                // Unavailable etcd endpoints
                ComputerOptions.BSP_ETCD_ENDPOINTS, "http://abc:8098",
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.BSP_LOG_INTERVAL, "30000",
                ComputerOptions.BSP_MAX_SUPER_STEP, "2"
        );

        Config config = ComputerContext.instance().config();
        Assert.assertThrows(ComputerException.class, () -> {
            workerService.init(config);
            workerService.execute();
        }, e -> {
            Assert.assertContains("Error while putting", e.getMessage());
            Assert.assertContains("UNAVAILABLE: unresolved address",
                                  e.getCause().getMessage());
        });
    }

    @Test
    public void testDataTransportManagerFail() throws InterruptedException {
        /*
         * TODO: Complete this test case after data transport manager is
         *  completed.
         */
    }
}
