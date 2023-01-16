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

package org.apache.hugegraph.computer.suite.integrate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.hugegraph.computer.algorithm.centrality.pagerank.PageRankParams;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.manager.Managers;
import org.apache.hugegraph.computer.core.master.MasterService;
import org.apache.hugegraph.computer.core.network.DataClientManager;
import org.apache.hugegraph.computer.core.network.connection.ConnectionManager;
import org.apache.hugegraph.computer.core.network.message.Message;
import org.apache.hugegraph.computer.core.network.netty.NettyTransportClient;
import org.apache.hugegraph.computer.core.network.session.ClientSession;
import org.apache.hugegraph.computer.core.util.ComputerContextUtil;
import org.apache.hugegraph.computer.core.worker.WorkerService;
import org.apache.hugegraph.config.RpcOptions;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.Log;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import com.google.common.collect.Sets;

public class SenderIntegrateTest {

    public static final Logger LOG = Log.logger(SenderIntegrateTest.class);

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
    public void testOneWorker() {
        AtomicReference<MasterService> masterServiceRef = new AtomicReference<>();
        AtomicReference<WorkerService> workerServiceRef = new AtomicReference<>();
        CompletableFuture<Void> masterFuture = new CompletableFuture<>();
        Thread masterThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withAlgorithm(PageRankParams.class)
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(1)
                                          .withBufferThreshold(50)
                                          .withBufferCapacity(60)
                                          .withRpcServerHost("127.0.0.1")
                                          .withRpcServerPort(8611)
                                          .withRpcServerPort(0)
                                          .build();
            try (MasterService service = initMaster(args)) {
                masterServiceRef.set(service);
                service.execute();
                masterFuture.complete(null);
            } catch (Exception e) {
                LOG.error("Failed to execute master service", e);
                masterFuture.completeExceptionally(e);
            }
        });
        masterThread.setDaemon(true);

        CompletableFuture<Void> workerFuture = new CompletableFuture<>();
        Thread workerThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withAlgorithm(PageRankParams.class)
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(1)
                                          .withBufferThreshold(50)
                                          .withBufferCapacity(60)
                                          .withTransoprtServerPort(0)
                                          .build();
            try (WorkerService service = initWorker(args)) {
                workerServiceRef.set(service);
                service.execute();
                workerFuture.complete(null);
            } catch (Throwable e) {
                LOG.error("Failed to execute worker service", e);
                workerFuture.completeExceptionally(e);
            }
        });
        workerThread.setDaemon(true);
        masterThread.start();
        workerThread.start();

        try {
            CompletableFuture.allOf(workerFuture, masterFuture).join();
        } finally {
            workerServiceRef.get().close();
            masterServiceRef.get().close();
        }
    }

    @Test
    public void testMultiWorkers() throws IOException {
        int workerCount = 3;
        int partitionCount = 3;
        AtomicReference<MasterService> masterServiceRef = new AtomicReference<>();
        Set<WorkerService> workerServices = Sets.newConcurrentHashSet();
        CompletableFuture<Void> masterFuture = new CompletableFuture<>();
        Thread masterThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_003")
                                          .withAlgorithm(PageRankParams.class)
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(workerCount)
                                          .withPartitionCount(partitionCount)
                                          .withRpcServerHost("127.0.0.1")
                                          .withRpcServerPort(0)
                                          .build();
            try {
                MasterService service = initMaster(args);
                masterServiceRef.set(service);
                service.execute();
                masterFuture.complete(null);
            } catch (Throwable e) {
                LOG.error("Failed to execute master service", e);
                masterFuture.completeExceptionally(e);
            }
        });
        masterThread.setDaemon(true);

        Map<Thread, CompletableFuture<Void>> workers = new HashMap<>(workerCount);
        for (int i = 1; i <= workerCount; i++) {
            String dir = "[jobs-" + i + "]";

            CompletableFuture<Void> workerFuture = new CompletableFuture<>();
            Thread thread = new Thread(() -> {
                String[] args;
                args = OptionsBuilder.newInstance()
                        .withJobId("local_003")
                        .withAlgorithm(PageRankParams.class)
                        .withResultName("rank")
                        .withResultClass(DoubleValue.class)
                        .withMessageClass(DoubleValue.class)
                        .withMaxSuperStep(3)
                        .withComputationClass(COMPUTATION)
                        .withWorkerCount(workerCount)
                        .withPartitionCount(partitionCount)
                        .withTransoprtServerPort(0)
                        .withDataDirs(dir)
                        .build();
                try {
                    WorkerService service = initWorker(args);
                    workerServices.add(service);
                    service.execute();
                    workerFuture.complete(null);
                } catch (Throwable e) {
                    LOG.error("Failed to execute worker service", e);
                    workerFuture.completeExceptionally(e);
                }
            });
            thread.setDaemon(true);
            workers.put(thread, workerFuture);
        }

        masterThread.start();
        for (Thread worker : workers.keySet()) {
            worker.start();
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>(workers.values());
        futures.add(masterFuture);
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } finally {
            for (WorkerService workerService : workerServices) {
                workerService.close();
            }
            masterServiceRef.get().close();
        }
    }

    @Test
    public void testOneWorkerWithBusyClient() {
        AtomicReference<MasterService> masterServiceRef = new AtomicReference<>();
        AtomicReference<WorkerService> workerServiceRef = new AtomicReference<>();
        CompletableFuture<Void> masterFuture = new CompletableFuture<>();
        Thread masterThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withAlgorithm(PageRankParams.class)
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(1)
                                          .withWriteBufferHighMark(10)
                                          .withWriteBufferLowMark(5)
                                          .withRpcServerHost("127.0.0.1")
                                          .withRpcServerPort(0)
                                          .build();
            try (MasterService service = initMaster(args)) {
                masterServiceRef.set(service);
                service.execute();
                masterFuture.complete(null);
            } catch (Throwable e) {
                LOG.error("Failed to execute master service", e);
                masterFuture.completeExceptionally(e);
            }
        });
        masterThread.setDaemon(true);

        CompletableFuture<Void> workerFuture = new CompletableFuture<>();
        int transoprtServerPort = 8998;
        Thread workerThread = new Thread(() -> {
            String[] args = OptionsBuilder.newInstance()
                                          .withJobId("local_002")
                                          .withAlgorithm(PageRankParams.class)
                                          .withResultName("rank")
                                          .withResultClass(DoubleValue.class)
                                          .withMessageClass(DoubleValue.class)
                                          .withMaxSuperStep(3)
                                          .withComputationClass(COMPUTATION)
                                          .withWorkerCount(1)
                                          .withWriteBufferHighMark(20)
                                          .withWriteBufferLowMark(10)
                                          .withTransoprtServerPort(transoprtServerPort)
                                          .build();
            try (WorkerService service = initWorker(args)) {
                workerServiceRef.set(service);
                // Let send rate slowly
                this.slowSendFunc(service, transoprtServerPort);
                service.execute();
                workerFuture.complete(null);
            } catch (Throwable e) {
                LOG.error("Failed to execute worker service", e);
                workerFuture.completeExceptionally(e);
            }
        });
        workerThread.setDaemon(true);
        masterThread.start();
        workerThread.start();

        try {
            CompletableFuture.allOf(workerFuture, masterFuture).join();
        } finally {
            workerServiceRef.get().close();
            masterServiceRef.get().close();
        }
    }

    private void slowSendFunc(WorkerService service, int port) throws TransportException {
        Managers managers = Whitebox.getInternalState(service, "managers");
        DataClientManager clientManager = managers.get(
                                          DataClientManager.NAME);
        ConnectionManager connManager = Whitebox.getInternalState(
                                        clientManager, "connManager");
        NettyTransportClient client = (NettyTransportClient)
                                      connManager.getOrCreateClient(
                                      "127.0.0.1", port);
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
        Config config = ComputerContextUtil.initContext(
                        ComputerContextUtil.convertToMap(args));
        MasterService service = new MasterService();
        service.init(config);
        return service;
    }

    private WorkerService initWorker(String[] args) {
        Config config = ComputerContextUtil.initContext(
                        ComputerContextUtil.convertToMap(args));
        WorkerService service = new WorkerService();
        service.init(config);
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

        public OptionsBuilder withAlgorithm(Class<?> clazz) {
            this.options.add(ComputerOptions.ALGORITHM_PARAMS_CLASS.name());
            this.options.add(clazz.getName());
            return this;
        }

        public OptionsBuilder withResultClass(Class<?> clazz) {
            this.options.add(ComputerOptions.ALGORITHM_RESULT_CLASS.name());
            this.options.add(clazz.getName());
            return this;
        }

        public OptionsBuilder withMessageClass(Class<?> clazz) {
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

        public OptionsBuilder withDataDirs(String dataDirs) {
            this.options.add(ComputerOptions.WORKER_DATA_DIRS.name());
            this.options.add(String.valueOf(dataDirs));
            return this;
        }
    }
}
