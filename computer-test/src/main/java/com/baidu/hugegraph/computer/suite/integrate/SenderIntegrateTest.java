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

package com.baidu.hugegraph.computer.suite.integrate;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.master.MasterService;
import com.baidu.hugegraph.computer.core.util.ComputerContextUtil;
import com.baidu.hugegraph.computer.core.worker.WorkerService;
import com.baidu.hugegraph.config.RpcOptions;
import com.baidu.hugegraph.testutil.Assert;

public class SenderIntegrateTest {

    private static final Class<?> COMPUTATION = MockComputation.class;

    @BeforeClass
    public static void init() {
    }

    @AfterClass
    public static void clear() {
        // pass
    }

    @Test
    public void testOneWorker() throws InterruptedException {
        Thread masterThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withValueName("rank")
                                          .withValueType("DOUBLE")
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(1)
                                          .withRpcServerHost("127.0.0.1")
                                          .withRpcServerPort(8090)
                                          .build();
            MasterService service = null;
            try {
                service = initMaster(args);
                service.execute();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            } finally {
                if (service != null) {
                    service.close();
                }
            }
        });
        Thread workerThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withValueName("rank")
                                          .withValueType("DOUBLE")
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(1)
                                          .withTransoprtServerPort(8091)
                                          .withRpcServerRemote("127.0.0.1:8090")
                                          .build();
            WorkerService service = null;
            try {
                Thread.sleep(2000);
                service = initWorker(args);
                service.execute();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            } finally {
                if (service != null) {
                    service.close();
                }
            }
        });
        masterThread.start();
        workerThread.start();

        workerThread.join();
        masterThread.join();
    }

    @Test
    public void testTwoWorkers() throws InterruptedException {
        Thread masterThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_003")
                                          .withValueName("rank")
                                          .withValueType("DOUBLE")
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(2)
                                          .withPartitionCount(2)
                                          .withRpcServerHost("127.0.0.1")
                                          .withRpcServerPort(8090)
                                          .build();
            try {
                MasterService service = initMaster(args);
                service.execute();
                service.close();
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        });
        Thread workerThread1 = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_003")
                                          .withValueName("rank")
                                          .withValueType("DOUBLE")
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(2)
                                          .withPartitionCount(2)
                                          .withTransoprtServerPort(8091)
                                          .withRpcServerRemote("127.0.0.1:8090")
                                          .build();
            try {
                Thread.sleep(2000);
                WorkerService service = initWorker(args);
                service.execute();
                service.close();
            } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
            }
        });
        Thread workerThread2 = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_003")
                                          .withValueName("rank")
                                          .withValueType("DOUBLE")
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(2)
                                          .withPartitionCount(2)
                                          .withTransoprtServerPort(8092)
                                          .withRpcServerRemote("127.0.0.1:8090")
                                          .build();
            try {
                Thread.sleep(2000);
                WorkerService service = initWorker(args);
                service.execute();
                service.close();
            } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
            }
        });

        masterThread.start();
        workerThread1.start();
        workerThread2.start();

        workerThread1.join();
        workerThread2.join();
        masterThread.join();
    }

    private MasterService initMaster(String[] args) {
        ComputerContextUtil.initContext(args);
        ComputerContext context = ComputerContext.instance();
        MasterService service = new MasterService();
        service.init(context.config());
        return service;
    }

    private WorkerService initWorker(String[] args) {
        ComputerContextUtil.initContext(args);
        ComputerContext context = ComputerContext.instance();
        WorkerService service = new WorkerService();
        service.init(context.config());
        return service;
    }

    private static class OptionsBuilder {

        private final List<String> options;

        public static OptionsBuilder newInstance() {
            return new OptionsBuilder();
        }

        public OptionsBuilder() {
            this.options = new ArrayList<>();
        }

        public String[] build() {
            return this.options.toArray(new String[0]);
        }

        public OptionsBuilder withJobId(String jobId) {
            this.options.add(ComputerOptions.JOB_ID.name());
            this.options.add(jobId);
            return this;
        }

        public OptionsBuilder withValueName(String name) {
            this.options.add(ComputerOptions.VALUE_NAME.name());
            this.options.add(name);
            return this;
        }

        public OptionsBuilder withValueType(String type) {
            this.options.add(ComputerOptions.VALUE_TYPE.name());
            this.options.add(type);
            return this;
        }

        public OptionsBuilder withMaxSuperStep(int maxSuperStep) {
            this.options.add(ComputerOptions.BSP_MAX_SUPER_STEP.name());
            this.options.add(String.valueOf(maxSuperStep));
            return this;
        }

        public OptionsBuilder withComputationClass(Class<?> clazz) {
            this.options.add(ComputerOptions.WORKER_COMPUTATION_CLASS.name());
            this.options.add(clazz.getName());
            return this;
        }

        public OptionsBuilder withWorkerCount(int count) {
            this.options.add(ComputerOptions.JOB_WORKERS_COUNT.name());
            this.options.add(String.valueOf(count));
            return this;
        }

        public OptionsBuilder withPartitionCount(int count) {
            this.options.add(ComputerOptions.JOB_PARTITIONS_COUNT.name());
            this.options.add(String.valueOf(count));
            return this;
        }

        public OptionsBuilder withTransoprtServerPort(int dataPort) {
            this.options.add(ComputerOptions.TRANSPORT_SERVER_PORT.name());
            this.options.add(String.valueOf(dataPort));
            return this;
        }

        public OptionsBuilder withRpcServerHost(String host) {
            this.options.add(RpcOptions.RPC_SERVER_HOST.name());
            this.options.add(host);
            return this;
        }

        public OptionsBuilder withRpcServerPort(int port) {
            this.options.add(RpcOptions.RPC_SERVER_PORT.name());
            this.options.add(String.valueOf(port));
            return this;
        }

        public OptionsBuilder withRpcServerRemote(String remoteUrl) {
            this.options.add(RpcOptions.RPC_REMOTE_URL.name());
            this.options.add(remoteUrl);
            return this;
        }
    }
}
