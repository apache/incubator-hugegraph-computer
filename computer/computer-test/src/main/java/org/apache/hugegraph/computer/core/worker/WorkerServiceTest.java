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

package org.apache.hugegraph.computer.core.worker;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.master.MasterService;
import org.apache.hugegraph.computer.core.output.LimitedLogOutput;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.config.RpcOptions;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Log;
import org.junit.Test;
import org.slf4j.Logger;

public class WorkerServiceTest extends UnitTestBase {

    private static final Logger LOG = Log.logger(WorkerServiceTest.class);

    @Test
    public void testServiceWith1Worker() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        Throwable[] exceptions = new Throwable[2];

        pool.submit(() -> {
            Config config = UnitTestBase.updateWithRequiredOptions(
                    ComputerOptions.JOB_ID, "local_002",
                    ComputerOptions.JOB_WORKERS_COUNT, "1",
                    ComputerOptions.TRANSPORT_SERVER_PORT, "8086",
                    ComputerOptions.BSP_REGISTER_TIMEOUT, "100000",
                    ComputerOptions.BSP_LOG_INTERVAL, "30000",
                    ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                    ComputerOptions.WORKER_COMPUTATION_CLASS,
                    MockComputation.class.getName(),
                    ComputerOptions.ALGORITHM_RESULT_CLASS,
                    DoubleValue.class.getName(),
                    ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                    DoubleValue.class.getName(),
                    ComputerOptions.OUTPUT_CLASS,
                    LimitedLogOutput.class.getName()
            );
            try (WorkerService workerService = new MockWorkerService()) {
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
            Config config = UnitTestBase.updateWithRequiredOptions(
                    RpcOptions.RPC_SERVER_HOST, "localhost",
                    ComputerOptions.JOB_ID, "local_002",
                    ComputerOptions.JOB_WORKERS_COUNT, "1",
                    ComputerOptions.BSP_REGISTER_TIMEOUT, "100000",
                    ComputerOptions.BSP_LOG_INTERVAL, "30000",
                    ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                    ComputerOptions.MASTER_COMPUTATION_CLASS,
                    MockMasterComputation.class.getName(),
                    ComputerOptions.ALGORITHM_RESULT_CLASS,
                    DoubleValue.class.getName(),
                    ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                    DoubleValue.class.getName()
            );
            try (MasterService masterService = new MasterService()) {
                masterService.init(config);
                masterService.execute();
            } catch (Throwable e) {
                LOG.error("Failed to start master", e);
                exceptions[1] = e;
            } finally {
                /*
                 * It must close the service first. The pool will be shutdown
                 * if count down is executed first, and the server thread in
                 * master service will not be closed.
                 */
                countDownLatch.countDown();
            }
        });

        countDownLatch.await();
        pool.shutdownNow();

        Assert.assertFalse(Arrays.asList(exceptions).toString(), existError(exceptions));
    }

    @Test
    public void testServiceWith2Workers() throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch countDownLatch = new CountDownLatch(3);
        Throwable[] exceptions = new Throwable[3];

        pool.submit(() -> {
            Config config = UnitTestBase.updateWithRequiredOptions(
                    ComputerOptions.JOB_ID, "local_003",
                    ComputerOptions.JOB_WORKERS_COUNT, "2",
                    ComputerOptions.JOB_PARTITIONS_COUNT, "2",
                    ComputerOptions.TRANSPORT_SERVER_PORT, "8086",
                    ComputerOptions.WORKER_DATA_DIRS, "[job_8086]",
                    ComputerOptions.BSP_REGISTER_TIMEOUT, "30000",
                    ComputerOptions.BSP_LOG_INTERVAL, "10000",
                    ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                    ComputerOptions.WORKER_COMPUTATION_CLASS,
                    MockComputation2.class.getName(),
                    ComputerOptions.ALGORITHM_RESULT_CLASS,
                    DoubleValue.class.getName(),
                    ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                    DoubleValue.class.getName()
            );

            try (WorkerService workerService = new MockWorkerService()) {
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
            Config config = UnitTestBase.updateWithRequiredOptions(
                    ComputerOptions.JOB_ID, "local_003",
                    ComputerOptions.JOB_WORKERS_COUNT, "2",
                    ComputerOptions.JOB_PARTITIONS_COUNT, "2",
                    ComputerOptions.TRANSPORT_SERVER_PORT, "8087",
                    ComputerOptions.WORKER_DATA_DIRS, "[job_8087]",
                    ComputerOptions.BSP_REGISTER_TIMEOUT, "30000",
                    ComputerOptions.BSP_LOG_INTERVAL, "10000",
                    ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                    ComputerOptions.WORKER_COMPUTATION_CLASS,
                    MockComputation2.class.getName(),
                    ComputerOptions.ALGORITHM_RESULT_CLASS,
                    DoubleValue.class.getName(),
                    ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                    DoubleValue.class.getName()
            );
            try (WorkerService workerService = new MockWorkerService()) {
                workerService.init(config);
                workerService.execute();
            } catch (Throwable e) {
                LOG.error("Failed to start worker", e);
                exceptions[1] = e;
            } finally {
                countDownLatch.countDown();
            }
        });

        pool.submit(() -> {
            Config config = UnitTestBase.updateWithRequiredOptions(
                    RpcOptions.RPC_SERVER_HOST, "localhost",
                    ComputerOptions.JOB_ID, "local_003",
                    ComputerOptions.JOB_WORKERS_COUNT, "2",
                    ComputerOptions.JOB_PARTITIONS_COUNT, "2",
                    ComputerOptions.BSP_REGISTER_TIMEOUT, "30000",
                    ComputerOptions.BSP_LOG_INTERVAL, "10000",
                    ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                    ComputerOptions.MASTER_COMPUTATION_CLASS,
                    MockMasterComputation2.class.getName(),
                    ComputerOptions.ALGORITHM_RESULT_CLASS,
                    DoubleValue.class.getName(),
                    ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                    DoubleValue.class.getName()
            );
            try (MasterService masterService = new MasterService()) {
                masterService.init(config);
                masterService.execute();
            } catch (Throwable e) {
                LOG.error("Failed to start master", e);
                exceptions[2] = e;
            } finally {
                countDownLatch.countDown();
            }
        });

        countDownLatch.await();
        pool.shutdownNow();

        Assert.assertFalse(Arrays.asList(exceptions).toString(), existError(exceptions));
    }

    @Test
    public void testFailToConnectEtcd() {
        Config config = UnitTestBase.updateWithRequiredOptions(
                // Unavailable etcd endpoints
                ComputerOptions.BSP_ETCD_ENDPOINTS, "http://invalid-ip:8098",
                ComputerOptions.JOB_ID, "local_004",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.BSP_LOG_INTERVAL, "30000",
                ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                ComputerOptions.WORKER_COMPUTATION_CLASS,
                MockComputation.class.getName()
        );

        try (WorkerService workerService = new MockWorkerService()) {
            Assert.assertThrows(ComputerException.class, () -> {
                workerService.init(config);
                workerService.execute();
            }, e -> {
                Assert.assertContains("Error while getting with key='BSP_MASTER_INIT_DONE'",
                                      e.getMessage());
                Assert.assertContains("UNAVAILABLE: unresolved address",
                                      e.getCause().getMessage());
            });
        }
    }

    @Test
    public void testDataTransportManagerFail() {
        /*
         * TODO: Complete this test case after data transport manager is
         *  completed.
         */
    }
}
