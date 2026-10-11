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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
import org.apache.hugegraph.testutil.Assert;
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
    private static final long BSP_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(90L);
    private static final long SERVICE_WAIT_TIMEOUT =
            BSP_TIMEOUT_MS + TimeUnit.SECONDS.toMillis(30L);

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
                                          // 4KB keeps the sort+send path exercised without
                                          // flooding the ACK-throttled transport (50B stalled)
                                          .withBufferThreshold(4096)
                                          .withBufferCapacity(8192)
                                          .withRpcServerHost("127.0.0.1")
                                          .withRpcServerPort(8611)
                                        .withRpcServerPort(0)
                                        .withTestBspTimeouts()
                                        .build();
            try (MasterService service = initMaster(
                                       args, masterServiceRef::set)) {
                service.execute();
                masterFuture.complete(null);
            } catch (Throwable e) {
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
                                          .withBufferThreshold(4096)
                                          .withBufferCapacity(8192)
                                        .withTransportServerPort(0)
                                        .withTestBspTimeouts()
                                        .build();
            try (WorkerService service = initWorker(
                                       args, workerServiceRef::set)) {
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

        Throwable failure = null;
        try {
            awaitServices(workerFuture, masterFuture);
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            closeServices(failure, workerServiceRef.get(),
                          masterServiceRef.get());
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
                                        .withTestBspTimeouts()
                                        .build();
            try {
                MasterService service = initMaster(
                                        args, masterServiceRef::set);
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
                   .withTransportServerPort(0)
                   .withDataDirs(dir)
                   .withTestBspTimeouts()
                   .build();
                try {
                    WorkerService service = initWorker(
                                            args, workerServices::add);
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
        Throwable failure = null;
        try {
            awaitServices(futures.toArray(new CompletableFuture[0]));
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            List<Closeable> services = new ArrayList<>(workerServices);
            services.add(masterServiceRef.get());
            closeServices(failure, services.toArray(new Closeable[0]));
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
                                        .withTestBspTimeouts()
                                        .build();
            try (MasterService service = initMaster(
                                       args, masterServiceRef::set)) {
                service.execute();
                masterFuture.complete(null);
            } catch (Throwable e) {
                LOG.error("Failed to execute master service", e);
                masterFuture.completeExceptionally(e);
            }
        });
        masterThread.setDaemon(true);

        CompletableFuture<Void> workerFuture = new CompletableFuture<>();
        int transportServerPort = 8998;
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
                                        .withTransportServerPort(
                                         transportServerPort)
                                        .withTestBspTimeouts()
                                        .build();
            try (WorkerService service = initWorker(
                                       args, workerServiceRef::set)) {
                // Let send rate slowly
                this.slowSendFunc(service, transportServerPort);
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

        Throwable failure = null;
        try {
            awaitServices(workerFuture, masterFuture);
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            closeServices(failure, workerServiceRef.get(),
                          masterServiceRef.get());
        }
    }

    @Test
    public void testFailedServiceReleasesPeerWaitFast() {
        CompletableFuture<Void> failed = new CompletableFuture<>();
        CompletableFuture<Void> unfinished = new CompletableFuture<>();
        IllegalStateException cause =
                new IllegalStateException("service failed");
        failed.completeExceptionally(cause);

        long start = System.currentTimeMillis();
        try {
            awaitServices(TimeUnit.SECONDS.toMillis(1L), failed, unfinished);
            Assert.fail("Expected CompletionException when a service fails");
        } catch (CompletionException e) {
            long elapsed = System.currentTimeMillis() - start;
            Assert.assertTrue(
                    "Wait should fail fast on first failure, but waited " +
                    elapsed + " ms",
                    elapsed < TimeUnit.SECONDS.toMillis(5L));
            Assert.assertSame(cause, e.getCause());
        }
    }

    private void slowSendFunc(WorkerService service, int port)
                              throws TransportException {
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
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return sendFuncBak.apply(message);
        };
        Whitebox.setInternalState(clientSession, "sendFunction", sendFunc);
    }

    private MasterService initMaster(String[] args,
                                     Consumer<MasterService> register) {
        Config config = ComputerContextUtil.initContext(
                        ComputerContextUtil.convertToMap(args));
        MasterService service = new MasterService();
        register.accept(service);
        service.init(config);
        return service;
    }

    private WorkerService initWorker(String[] args,
                                     Consumer<WorkerService> register) {
        Config config = ComputerContextUtil.initContext(
                        ComputerContextUtil.convertToMap(args));
        WorkerService service = new WorkerService();
        register.accept(service);
        service.init(config);
        return service;
    }

    private static void closeServices(Throwable primaryFailure,
                                      Closeable... services) {
        Throwable cleanupFailure = null;
        for (Closeable service : services) {
            if (service == null) {
                continue;
            }
            try {
                service.close();
            } catch (Throwable failure) {
                if (primaryFailure != null) {
                    primaryFailure.addSuppressed(failure);
                } else if (cleanupFailure == null) {
                    cleanupFailure = failure;
                } else {
                    cleanupFailure.addSuppressed(failure);
                }
            }
        }
        if (cleanupFailure instanceof RuntimeException) {
            throw (RuntimeException) cleanupFailure;
        }
        if (cleanupFailure instanceof Error) {
            throw (Error) cleanupFailure;
        }
        if (cleanupFailure != null) {
            throw new RuntimeException("Failed to close services",
                                       cleanupFailure);
        }
    }

    private static void awaitServices(CompletableFuture<Void>... futures) {
        awaitServices(SERVICE_WAIT_TIMEOUT, futures);
    }

    private static void awaitServices(long timeout,
                                      CompletableFuture<Void>... futures) {
        CompletableFuture<Void> allDone = CompletableFuture.allOf(futures);
        CompletableFuture<Void> firstFailure = new CompletableFuture<>();
        for (CompletableFuture<Void> future : futures) {
            future.whenComplete((result, error) -> {
                if (error != null) {
                    firstFailure.completeExceptionally(error);
                }
            });
        }
        CompletableFuture.anyOf(firstFailure, allDone)
                         .orTimeout(timeout, TimeUnit.MILLISECONDS)
                         .join();
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

        public OptionsBuilder withTransportServerPort(int dataPort) {
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

        public OptionsBuilder withTestBspTimeouts() {
            this.options.add(ComputerOptions.BSP_WAIT_WORKERS_TIMEOUT.name());
            this.options.add(String.valueOf(BSP_TIMEOUT_MS));
            this.options.add(ComputerOptions.BSP_WAIT_MASTER_TIMEOUT.name());
            this.options.add(String.valueOf(BSP_TIMEOUT_MS));
            return this;
        }

    }
}
