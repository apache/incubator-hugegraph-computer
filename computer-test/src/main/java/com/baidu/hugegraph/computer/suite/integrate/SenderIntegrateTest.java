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
import java.util.concurrent.Future;
import java.util.function.Function;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.master.MasterService;
import com.baidu.hugegraph.computer.core.network.DataClientManager;
import com.baidu.hugegraph.computer.core.network.connection.ConnectionManager;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.computer.core.network.netty.NettyTransportClient;
import com.baidu.hugegraph.computer.core.network.session.ClientSession;
import com.baidu.hugegraph.computer.core.util.ComputerContextUtil;
import com.baidu.hugegraph.computer.core.worker.WorkerService;
import com.baidu.hugegraph.config.RpcOptions;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;

public class SenderIntegrateTest {

    private static final Class<?> COMPUTATION = MockComputation.class;

    @BeforeClass
    public static void init() {
        // pass
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
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(1)
                                          .withBufferThreshold(50)
                                          .withBufferCapacity(60)
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
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(1)
                                          .withBufferThreshold(50)
                                          .withBufferCapacity(60)
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
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
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
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
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
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
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

    @Test
    public void testOneWorkerWithBusyClient() throws InterruptedException {
        Thread masterThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(1)
                                          .withWriteBufferHighMark(10)
                                          .withWriteBufferLowMark(5)
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
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(1)
                                          .withTransoprtServerPort(8091)
                                          .withWriteBufferHighMark(20)
                                          .withWriteBufferLowMark(10)
                                          .withRpcServerRemote("127.0.0.1:8090")
                                          .build();
            WorkerService service = null;
            try {
                Thread.sleep(2000);
                service = initWorker(args);
                // Let send rate slowly
                this.slowSendFunc(service);
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

    private void slowSendFunc(WorkerService service) throws TransportException {
        Managers managers = Whitebox.getInternalState(service, "managers");
        DataClientManager clientManager = managers.get(
                                          DataClientManager.NAME);
        ConnectionManager connManager = Whitebox.getInternalState(
                                        clientManager, "connManager");
        NettyTransportClient client = (NettyTransportClient)
                                      connManager.getOrCreateClient(
                                      "127.0.0.1", 8091);
        ClientSession clientSession = Whitebox.invoke(client.getClass(),
                                                      "clientSession", client);
        Function<Message, Future<Void>> sendFuncBak = Whitebox.getInternalState(
                                                      clientSession,
                                                      "sendFunction");
        Function<Message, Future<Void>> sendFunc = message -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return sendFuncBak.apply(message);
        };
        Whitebox.setInternalState(clientSession, "sendFunction", sendFunc);
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

        public OptionsBuilder withResultClass(Class clazz) {
            this.options.add(ComputerOptions.ALGORITHM_RESULT_CLASS.name());
            this.options.add(clazz.getName());
            return this;
        }

        public OptionsBuilder withMessageClass(Class clazz) {
            this.options.add(ComputerOptions.ALGORITHM_MESSAGE_CLASS.name());
            this.options.add(clazz.getName());
            return this;
        }

        public OptionsBuilder withResultName(String name) {
            this.options.add(ComputerOptions.OUTPUT_RESULT_NAME.name());
            this.options.add(name);
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

        public OptionsBuilder withBufferThreshold(int sizeInByte) {
            this.options.add(ComputerOptions.WORKER_WRITE_BUFFER_THRESHOLD
                                            .name());
            this.options.add(String.valueOf(sizeInByte));
            return this;
        }

        public OptionsBuilder withBufferCapacity(int sizeInByte) {
            this.options.add(ComputerOptions.WORKER_WRITE_BUFFER_INIT_CAPACITY
                                            .name());
            this.options.add(String.valueOf(sizeInByte));
            return this;
        }

        public OptionsBuilder withTransoprtServerPort(int dataPort) {
            this.options.add(ComputerOptions.TRANSPORT_SERVER_PORT.name());
            this.options.add(String.valueOf(dataPort));
            return this;
        }

        public OptionsBuilder withWriteBufferHighMark(int mark) {
            this.options.add(ComputerOptions.TRANSPORT_WRITE_BUFFER_HIGH_MARK
                                            .name());
            this.options.add(String.valueOf(mark));
            return this;
        }

        public OptionsBuilder withWriteBufferLowMark(int mark) {
            this.options.add(ComputerOptions.TRANSPORT_WRITE_BUFFER_LOW_MARK
                                            .name());
            this.options.add(String.valueOf(mark));
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
