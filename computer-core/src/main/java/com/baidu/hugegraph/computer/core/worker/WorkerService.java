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

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.aggregator.Aggregator;
import com.baidu.hugegraph.computer.core.aggregator.WorkerAggrManager;
import com.baidu.hugegraph.computer.core.bsp.Bsp4Worker;
import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.ContainerInfo;
import com.baidu.hugegraph.computer.core.compute.ComputeManager;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.SuperstepStat;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.input.WorkerInputManager;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.network.DataClientManager;
import com.baidu.hugegraph.computer.core.network.DataServerManager;
import com.baidu.hugegraph.computer.core.network.connection.ConnectionManager;
import com.baidu.hugegraph.computer.core.network.connection.TransportConnectionManager;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvManager;
import com.baidu.hugegraph.computer.core.rpc.WorkerRpcManager;
import com.baidu.hugegraph.computer.core.sender.MessageSendManager;
import com.baidu.hugegraph.computer.core.sort.sorting.RecvSortManager;
import com.baidu.hugegraph.computer.core.sort.sorting.SendSortManager;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.computer.core.store.FileManager;
import com.baidu.hugegraph.computer.core.util.ShutdownHook;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class WorkerService implements Closeable {

    private static final Logger LOG = Log.logger(WorkerService.class);

    private final ComputerContext context;
    private final Managers managers;
    private final Map<Integer, ContainerInfo> workers;

    private volatile boolean inited;
    private volatile boolean closed;
    private Config config;
    private Bsp4Worker bsp4Worker;
    private ComputeManager computeManager;
    private ContainerInfo workerInfo;

    private Combiner<Value> combiner;

    private ContainerInfo masterInfo;

    private volatile ShutdownHook shutdownHook;
    private volatile Thread serviceThread;

    public WorkerService() {
        this.context = ComputerContext.instance();
        this.managers = new Managers();
        this.workers = new HashMap<>();
        this.inited = false;
        this.closed = false;
        this.shutdownHook = new ShutdownHook();
    }

    /**
     * Init worker service, create the managers used by worker service.
     */
    public void init(Config config) {
        E.checkArgument(!this.inited, "The %s has been initialized", this);

        this.serviceThread = Thread.currentThread();
        this.registerShutdownHook();

        this.config = config;

        this.workerInfo = new ContainerInfo();
        LOG.info("{} Start to initialize worker", this);

        this.bsp4Worker = new Bsp4Worker(this.config, this.workerInfo);

        /*
         * Keep the waitMasterInitDone() called before initManagers(),
         * in order to ensure master init() before worker managers init()
         */
        this.masterInfo = this.bsp4Worker.waitMasterInitDone();

        InetSocketAddress address = this.initManagers(this.masterInfo);
        this.workerInfo.updateAddress(address);

        Computation<?> computation = this.config.createObject(
                                     ComputerOptions.WORKER_COMPUTATION_CLASS);
        LOG.info("Loading computation '{}' in category '{}'",
                 computation.name(), computation.category());

        this.combiner = this.config.createObject(
                        ComputerOptions.WORKER_COMBINER_CLASS, false);
        if (this.combiner == null) {
            LOG.info("None combiner is provided for computation '{}'",
                     computation.name());
        } else {
            LOG.info("Combiner '{}' is provided for computation '{}'",
                     this.combiner.name(), computation.name());
        }

        LOG.info("{} register WorkerService", this);
        this.bsp4Worker.workerInitDone();
        List<ContainerInfo> workers = this.bsp4Worker.waitMasterAllInitDone();
        DataClientManager dm = this.managers.get(DataClientManager.NAME);
        for (ContainerInfo worker : workers) {
            this.workers.put(worker.id(), worker);
            dm.connect(worker.id(), worker.hostname(), worker.dataPort());
        }

        this.computeManager = new ComputeManager(this.context, this.managers);

        this.managers.initedAll(this.config);
        LOG.info("{} WorkerService initialized", this);
        this.inited = true;
    }

    private void registerShutdownHook() {
        this.shutdownHook.hook(() -> {
            this.stopServiceThread();
            this.cleanAndCloseBsp();
        });
    }

    /**
     * Stop the worker service. Stop the managers created in
     * {@link #init(Config)}.
     */
    @Override
    public void close() {
        this.checkInited();
        if (this.closed) {
            LOG.info("{} WorkerService had closed before", this);
            return;
        }

        this.computeManager.close();
        /*
         * Seems managers.closeAll() would do the following actions:
         * TODO: close the connection to other workers.
         * TODO: stop the connection to the master
         * TODO: stop the data transportation server.
         */
        this.managers.closeAll(this.config);

        this.bsp4Worker.workerCloseDone();
        this.bsp4Worker.close();
        this.shutdownHook.unhook();

        this.closed = true;
        LOG.info("{} WorkerService closed", this);
    }

    private void stopServiceThread() {
        if (this.serviceThread == null) {
            return;
        }

        try {
            this.serviceThread.interrupt();
        } catch (Throwable ignore) {
        }
    }

    private void cleanAndCloseBsp() {
        if (this.bsp4Worker == null) {
            return;
        }

        this.bsp4Worker.clean();
        this.bsp4Worker.close();
    }

    /**
     * Execute the superstep in worker. It first wait master witch superstep
     * to start from. And then do the superstep iteration until master's
     * superstepStat is inactive.
     */
    public void execute() {
        this.checkInited();

        LOG.info("{} WorkerService execute", this);

        // TODO: determine superstep if fail over is enabled.
        int superstep = this.bsp4Worker.waitMasterResumeDone();
        SuperstepStat superstepStat;
        if (superstep == Constants.INPUT_SUPERSTEP) {
            superstepStat = this.inputstep();
            superstep++;
        } else {
            // TODO: Get superstepStat from bsp service.
            superstepStat = null;
        }

        /*
         * The master determine whether to execute the next superstep. The
         * superstepStat is active while master decides to execute the next
         * superstep.
         */
        while (superstepStat.active()) {
            WorkerContext context = new SuperstepContext(superstep,
                                                         superstepStat);
            LOG.info("Start computation of superstep {}", superstep);
            if (superstep > 0) {
                this.computeManager.takeRecvedMessages();
            }
            /*
             * Call beforeSuperstep() before all workers compute() called.
             *
             * NOTE: keep computeManager.compute() called after
             * managers.beforeSuperstep().
             */
            this.managers.beforeSuperstep(this.config, superstep);

            /*
             * Notify master by each worker, when the master received all
             * workers signal, then notify all workers to do compute().
             */
            this.bsp4Worker.workerStepPrepareDone(superstep);
            this.bsp4Worker.waitMasterStepPrepareDone(superstep);
            WorkerStat workerStat = this.computeManager.compute(context,
                                                                superstep);

            this.bsp4Worker.workerStepComputeDone(superstep);
            this.bsp4Worker.waitMasterStepComputeDone(superstep);

            /*
             * Call afterSuperstep() after all workers compute() is done.
             *
             * NOTE: keep managers.afterSuperstep() called after
             * computeManager.compute(), because managers may rely on
             * computation, like WorkerAggrManager send aggregators to master
             * after called aggregateValue(String name, V value) in computation.
             */
            this.managers.afterSuperstep(this.config, superstep);

            this.bsp4Worker.workerStepDone(superstep, workerStat);
            LOG.info("End computation of superstep {}", superstep);

            superstepStat = this.bsp4Worker.waitMasterStepDone(superstep);
            superstep++;
        }
        this.outputstep();
    }

    @Override
    public String toString() {
        Object id = this.workerInfo == null ?
                    "?" + this.hashCode() : this.workerInfo.id();
        return String.format("[worker %s]", id);
    }

    private InetSocketAddress initManagers(ContainerInfo masterInfo) {
        // Create managers
        WorkerRpcManager rpcManager = new WorkerRpcManager();
        this.managers.add(rpcManager);
        /*
         * NOTE: this init() method will be called twice, will be ignored at
         * the 2nd time call.
         */
        WorkerRpcManager.updateRpcRemoteServerConfig(this.config,
                                                     masterInfo.hostname(),
                                                     masterInfo.rpcPort());
        rpcManager.init(this.config);

        WorkerAggrManager aggregatorManager = new WorkerAggrManager(
                                              this.context);
        aggregatorManager.service(rpcManager.aggregateRpcService());
        this.managers.add(aggregatorManager);
        FileManager fileManager = new FileManager();
        this.managers.add(fileManager);

        SortManager recvSortManager = new RecvSortManager(this.context);
        this.managers.add(recvSortManager);

        MessageRecvManager recvManager = new MessageRecvManager(this.context,
                                         fileManager, recvSortManager);
        this.managers.add(recvManager);

        ConnectionManager connManager = new TransportConnectionManager();
        DataServerManager serverManager = new DataServerManager(connManager,
                                                                recvManager);
        this.managers.add(serverManager);
        this.managers.add(serverManager);

        DataClientManager clientManager = new DataClientManager(connManager,
                                                                this.context);
        this.managers.add(clientManager);

        SortManager sendSortManager = new SendSortManager(this.context);
        this.managers.add(sendSortManager);

        MessageSendManager sendManager = new MessageSendManager(this.context,
                                         sendSortManager,
                                         clientManager.sender());
        this.managers.add(sendManager);

        WorkerInputManager inputManager = new WorkerInputManager(this.context,
                                                                 sendManager);
        inputManager.service(rpcManager.inputSplitService());
        this.managers.add(inputManager);

        // Init all managers
        this.managers.initAll(this.config);

        InetSocketAddress address = serverManager.address();
        LOG.info("{} WorkerService initialized managers with data server " +
                 "address '{}'", this, address);
        return address;
    }

    private void checkInited() {
        E.checkArgument(this.inited, "The %s has not been initialized", this);
    }

    /**
     * Load vertices and edges from HugeGraph. There are two phases in
     * inputstep. First phase is get input splits from master, and read the
     * vertices and edges from input splits. The second phase is after all
     * workers read input splits, the workers merge the vertices and edges to
     * get the stats for each partition.
     */
    private SuperstepStat inputstep() {
        LOG.info("{} WorkerService inputstep started", this);
        WorkerInputManager manager = this.managers.get(WorkerInputManager.NAME);
        manager.loadGraph();

        this.bsp4Worker.workerInputDone();
        this.bsp4Worker.waitMasterInputDone();

        WorkerStat workerStat = this.computeManager.input();

        this.bsp4Worker.workerStepDone(Constants.INPUT_SUPERSTEP,
                                       workerStat);
        SuperstepStat superstepStat = this.bsp4Worker.waitMasterStepDone(
                                      Constants.INPUT_SUPERSTEP);
        manager.close(this.config);
        LOG.info("{} WorkerService inputstep finished", this);
        return superstepStat;
    }

    /**
     * Write results back parallel to HugeGraph and signal the master. Be
     * called after all superstep iteration finished. After this, this worker
     * can exit successfully.
     */
    private void outputstep() {
        this.computeManager.output();
        this.bsp4Worker.workerOutputDone();
        LOG.info("{} WorkerService outputstep finished", this);
    }

    private class SuperstepContext implements WorkerContext {

        private final int superstep;
        private final SuperstepStat superstepStat;
        private final WorkerAggrManager aggrManager;
        private final MessageSendManager sendManager;

        private SuperstepContext(int superstep, SuperstepStat superstepStat) {
            this.superstep = superstep;
            this.superstepStat = superstepStat;
            this.aggrManager = WorkerService.this.managers.get(
                               WorkerAggrManager.NAME);
            this.sendManager = WorkerService.this.managers.get(
                               MessageSendManager.NAME);
        }

        @Override
        public Config config() {
            return WorkerService.this.config;
        }

        @Override
        public <V extends Value> Aggregator<V> createAggregator(String name) {
            return this.aggrManager.createAggregator(name);
        }

        @Override
        public <V extends Value> void aggregateValue(String name, V value) {
            this.aggrManager.aggregateValue(name, value);
        }

        @Override
        public <V extends Value> V aggregatedValue(String name) {
            return this.aggrManager.aggregatedValue(name);
        }

        @Override
        public void sendMessage(Id target, Value value) {
            this.sendManager.sendMessage(target, value);
        }

        @Override
        public void sendMessageToAllEdges(Vertex vertex, Value value) {
            for (Edge edge : vertex.edges()) {
                this.sendMessage(edge.targetId(), value);
            }
        }

        @Override
        public long totalVertexCount() {
            return this.superstepStat.vertexCount();
        }

        @Override
        public long totalEdgeCount() {
            return this.superstepStat.edgeCount();
        }

        @Override
        public int superstep() {
            return this.superstep;
        }

        /**
         * Message combiner.
         */
        @Override
        @SuppressWarnings("unchecked")
        public <V extends Value> Combiner<V> combiner() {
            return (Combiner<V>) WorkerService.this.combiner;
        }
    }
}
