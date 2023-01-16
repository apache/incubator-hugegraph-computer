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

package org.apache.hugegraph.computer.core.bsp;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hugegraph.computer.core.common.ContainerInfo;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.SuperstepStat;
import org.apache.hugegraph.computer.core.graph.partition.PartitionStat;
import org.apache.hugegraph.computer.core.worker.WorkerStat;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EtcdBspTest extends UnitTestBase {

    private Bsp4Master bsp4Master;
    private Bsp4Worker bsp4Worker;
    private ContainerInfo masterInfo;
    private ContainerInfo workerInfo;
    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    private int maxSuperStep;

    @Before
    public void setup() {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.BSP_LOG_INTERVAL, "30000",
            ComputerOptions.BSP_MAX_SUPER_STEP, "2"
        );

        this.bsp4Master = new Bsp4Master(config);
        this.bsp4Master.clean();
        this.masterInfo = new ContainerInfo(-1, "localhost", 8001, 8002);
        this.workerInfo = new ContainerInfo(0, "localhost", 8003, 8004);
        this.bsp4Worker = new Bsp4Worker(config, this.workerInfo);
        this.maxSuperStep = config.get(ComputerOptions.BSP_MAX_SUPER_STEP);
    }

    @After
    public void teardown() {
        this.bsp4Worker.clean();
        this.bsp4Worker.close();
        this.bsp4Master.clean();
        this.bsp4Master.close();
    }

    @Test
    public void testInit() throws InterruptedException {
        // If both two threads reach countDown, it means no exception is thrown.
        CountDownLatch countDownLatch = new CountDownLatch(2);
        this.executorService.submit(() -> {
            this.bsp4Master.masterInitDone(this.masterInfo);
            List<ContainerInfo> workers = this.bsp4Master.waitWorkersInitDone();
            Assert.assertEquals(1, workers.size());
            countDownLatch.countDown();
        });
        this.executorService.submit(() -> {
            this.bsp4Worker.workerInitDone();
            ContainerInfo master = this.bsp4Worker.waitMasterInitDone();
            Assert.assertEquals(this.masterInfo, master);
            List<ContainerInfo> workers = this.bsp4Worker
                                              .waitMasterAllInitDone();
            Assert.assertEquals(1, workers.size());
            Assert.assertEquals(this.workerInfo, workers.get(0));
            countDownLatch.countDown();
        });
        countDownLatch.await();
    }

    @Test
    public void testInput() throws InterruptedException {
        // If both two threads reach countDown, it means no exception is thrown.
        WorkerStat workerStat = new WorkerStat();
        workerStat.add(new PartitionStat(0, 100L, 200L, 0L));
        workerStat.add(new PartitionStat(1, 200L, 300L, 0L));
        CountDownLatch countDownLatch = new CountDownLatch(2);
        this.executorService.submit(() -> {
            this.bsp4Master.masterResumeDone(-1);
            this.bsp4Master.waitWorkersInputDone();
            this.bsp4Master.masterInputDone();
            List<WorkerStat> workerStats = this.bsp4Master
                                               .waitWorkersStepDone(-1);
            Assert.assertEquals(1, workerStats.size());
            Assert.assertEquals(workerStat, workerStats.get(0));
            countDownLatch.countDown();
        });
        this.executorService.submit(() -> {
            int firstSuperStep = this.bsp4Worker.waitMasterResumeDone();
            Assert.assertEquals(-1, firstSuperStep);
            this.bsp4Worker.workerInputDone();
            this.bsp4Worker.waitMasterInputDone();
            this.bsp4Worker.workerStepDone(-1, workerStat);
            countDownLatch.countDown();
        });
        countDownLatch.await();
    }

    @Test
    public void testIterate() throws InterruptedException {
        // If both two threads reach countDown, it means no exception is thrown.
        WorkerStat workerStat = new WorkerStat();
        workerStat.add(new PartitionStat(0, 100L, 200L, 0L));
        workerStat.add(new PartitionStat(1, 200L, 300L, 0L));
        CountDownLatch countDownLatch = new CountDownLatch(2);
        this.executorService.submit(() -> {
            for (int i = 0; i < this.maxSuperStep; i++) {
                this.bsp4Master.waitWorkersStepPrepareDone(i);
                this.bsp4Master.masterStepPrepareDone(i);
                this.bsp4Master.waitWorkersStepComputeDone(i);
                this.bsp4Master.masterStepComputeDone(i);
                List<WorkerStat> list = this.bsp4Master.waitWorkersStepDone(i);
                SuperstepStat superstepStat = new SuperstepStat();
                for (WorkerStat workerStat1 : list) {
                    superstepStat.increase(workerStat1);
                }
                if (i == this.maxSuperStep - 1) {
                    superstepStat.inactivate();
                }
                this.bsp4Master.masterStepDone(i, superstepStat);
            }
            countDownLatch.countDown();
        });
        this.executorService.submit(() -> {
            int superstep = -1;
            SuperstepStat superstepStat = null;
            while (superstepStat == null || superstepStat.active()) {
                superstep++;
                this.bsp4Worker.workerStepPrepareDone(superstep);
                this.bsp4Worker.waitMasterStepPrepareDone(superstep);
                this.bsp4Worker.workerStepComputeDone(superstep);
                this.bsp4Worker.waitMasterStepComputeDone(superstep);
                PartitionStat stat1 = new PartitionStat(0, 100L, 200L, 50L);
                PartitionStat stat2 = new PartitionStat(1, 200L, 300L, 80L);
                WorkerStat workerStatInSuperstep = new WorkerStat();
                workerStatInSuperstep.add(stat1);
                workerStatInSuperstep.add(stat2);
                // Sleep some time to simulate the worker do computation.
                UnitTestBase.sleep(100L);
                this.bsp4Worker.workerStepDone(superstep,
                                               workerStatInSuperstep);
                superstepStat = this.bsp4Worker.waitMasterStepDone(superstep);
            }
            countDownLatch.countDown();
        });
        countDownLatch.await();
    }

    @Test
    public void testOutput() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        this.executorService.submit(() -> {
            this.bsp4Master.waitWorkersOutputDone();
            this.bsp4Master.clean();
            countDownLatch.countDown();

        });
        this.executorService.submit(() -> {
            this.bsp4Worker.workerOutputDone();
            countDownLatch.countDown();
        });
        countDownLatch.await();
    }
}
