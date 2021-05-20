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
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.SuperstepStat;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.input.WorkerInputManager;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.rpc.WorkerRpcManager;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class WorkerService {

    private static final Logger LOG = Log.logger(WorkerService.class);

    private final ComputerContext context;
    private final Managers managers;
    private final Map<Integer, ContainerInfo> workers;

    private boolean inited;
    private Config config;
    private Bsp4Worker bsp4Worker;
    private ContainerInfo workerInfo;
    private Computation<?> computation;
    private Combiner<Value<?>> combiner;

    private ContainerInfo masterInfo;

    public WorkerService() {
        this.context = ComputerContext.instance();
        this.managers = new Managers();
        this.workers = new HashMap<>();
        this.inited = false;
    }

    /**
     * Init worker service, create the managers used by worker service.
     */
    public void init(Config config) {
        E.checkArgument(!this.inited, "The %s has been initialized", this);

        this.config = config;

        InetSocketAddress dataAddress = this.initDataTransportManagers();

        this.workerInfo = new ContainerInfo(dataAddress.getHostName(),
                                            0, dataAddress.getPort());
        LOG.info("{} Start to initialize worker", this);

        this.bsp4Worker = new Bsp4Worker(this.config, this.workerInfo);

        /*
         * Keep the waitMasterInitDone() called before initManagers(),
         * in order to ensure master init() before worker managers init()
         */
        this.masterInfo = this.bsp4Worker.waitMasterInitDone();

        this.initManagers(this.masterInfo);

        this.computation = this.config.createObject(
                           ComputerOptions.WORKER_COMPUTATION_CLASS);
        this.computation.init(this.config);
        LOG.info("Loading computation '{}' in category '{}'",
                 this.computation.name(), this.computation.category());

        this.combiner = this.config.createObject(
                        ComputerOptions.WORKER_COMBINER_CLASS, false);
        if (this.combiner == null) {
            LOG.info("None combiner is provided for computation '{}'",
                     this.computation.name());
        } else {
            LOG.info("Combiner '{}' is provided for computation '{}'",
                     this.combiner.name(), this.computation.name());
        }

        LOG.info("{} register WorkerService", this);
        this.bsp4Worker.workerInitDone();
        List<ContainerInfo> workers = this.bsp4Worker.waitMasterAllInitDone();
        for (ContainerInfo worker : workers) {
            this.workers.put(worker.id(), worker);
            // TODO: Connect to other workers for data transport
            //DataClientManager dm = this.managers.get(DataClientManager.NAME);
            //dm.connect(container.hostname(), container.dataPort());
        }

        this.managers.initedAll(this.config);
        LOG.info("{} WorkerService initialized", this);
        this.inited = true;
    }

    /**
     * Stop the worker service. Stop the managers created in
     * {@link #init(Config)}.
     */
    public void close() {
        this.checkInited();

        this.computation.close(this.config);

        /*
         * Seems managers.closeAll() would do the following actions:
         * TODO: close the connection to other workers.
         * TODO: stop the connection to the master
         * TODO: stop the data transportation server.
         */
        this.managers.closeAll(this.config);

        this.bsp4Worker.workerCloseDone();
        this.bsp4Worker.close();
        LOG.info("{} WorkerService closed", this);
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

            /*
             * Call beforeSuperstep() before all workers compute() called.
             *
             * NOTE: keep computation.beforeSuperstep() called after
             * managers.beforeSuperstep().
             */
            this.managers.beforeSuperstep(this.config, superstep);
            this.computation.beforeSuperstep(context);

            /*
             * Notify master by each worker, when the master received all
             * workers signal, then notify all workers to do compute().
             */
            this.bsp4Worker.workerStepPrepareDone(superstep);
            this.bsp4Worker.waitMasterStepPrepareDone(superstep);

            WorkerStat workerStat = this.compute();

            /*
             * Wait for all workers to do compute()
             */
            this.bsp4Worker.workerStepComputeDone(superstep);
            this.bsp4Worker.waitMasterStepComputeDone(superstep);

            /*
             * Call afterSuperstep() after all workers compute() is done.
             *
             * NOTE: keep managers.afterSuperstep() called after
             * computation.afterSuperstep(), because managers may rely on
             * computation, like WorkerAggrManager send aggregators to master
             * after called aggregateValue(String name, V value) in computation.
             */
            this.computation.afterSuperstep(context);
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

    private InetSocketAddress initDataTransportManagers() {
        // TODO: Start data-transport server and get its host and port.
        String host = this.config.get(ComputerOptions.TRANSPORT_SERVER_HOST);
        int port = this.config.get(ComputerOptions.TRANSPORT_SERVER_PORT);
        InetSocketAddress dataAddress = InetSocketAddress.createUnresolved(
                                        host, port);

        return dataAddress;
    }

    private void initManagers(ContainerInfo masterInfo) {
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

        WorkerInputManager inputManager = new WorkerInputManager();
        inputManager.service(rpcManager.inputSplitService());
        this.managers.add(inputManager);

        WorkerAggrManager aggregatorManager = new WorkerAggrManager(
                                              this.context);
        aggregatorManager.service(rpcManager.aggregateRpcService());
        this.managers.add(aggregatorManager);

        // Init managers
        this.managers.initAll(this.config);

        LOG.info("{} WorkerService initialized managers", this);
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
        /*
         * TODO: Load vertices and edges parallel.
         */
        WorkerInputManager manager = this.managers.get(WorkerInputManager.NAME);
        manager.loadGraph();

        this.bsp4Worker.workerInputDone();
        this.bsp4Worker.waitMasterInputDone();

        /*
         * Merge vertices and edges in each partition parallel,
         * and get the workerStat.
         */
        WorkerStat workerStat = manager.mergeGraph();

        this.bsp4Worker.workerStepDone(Constants.INPUT_SUPERSTEP,
                                       workerStat);
        SuperstepStat superstepStat = this.bsp4Worker.waitMasterStepDone(
                                      Constants.INPUT_SUPERSTEP);
        LOG.info("{} WorkerService inputstep finished", this);
        return superstepStat;
    }

    /**
     * Write results back parallel to HugeGraph and signal the master. Be
     * called after all superstep iteration finished. After this, this worker
     * can exit successfully.
     */
    private void outputstep() {
        /*
         * Write results back parallel
         */
        // TODO: output the vertices in partitions parallel
        this.bsp4Worker.workerOutputDone();
        LOG.info("{} WorkerService outputstep finished", this);
    }

    /**
     * Compute vertices of all partitions parallel in this worker.
     * Be called one time for a superstep.
     * @return WorkerStat
     */
    private WorkerStat compute() {
        // TODO: compute partitions parallel and get workerStat
        PartitionStat stat1 = new PartitionStat(0, 100L, 200L,
                                                50L, 60L, 70L);
        WorkerStat workerStat = new WorkerStat();
        workerStat.add(stat1);
        return workerStat;
    }

    private class SuperstepContext implements WorkerContext {

        private final int superstep;
        private final SuperstepStat superstepStat;
        private final WorkerAggrManager aggrManager;

        private SuperstepContext(int superstep, SuperstepStat superstepStat) {
            this.superstep = superstep;
            this.superstepStat = superstepStat;
            this.aggrManager = WorkerService.this.managers.get(
                               WorkerAggrManager.NAME);
        }

        @Override
        public Config config() {
            return WorkerService.this.config;
        }

        @Override
        public <V extends Value<?>> Aggregator<V> createAggregator(
                                                  String name) {
            return this.aggrManager.createAggregator(name);
        }

        @Override
        public <V extends Value<?>> void aggregateValue(String name, V value) {
            this.aggrManager.aggregateValue(name, value);
        }

        @Override
        public <V extends Value<?>> V aggregatedValue(String name) {
            return this.aggrManager.aggregatedValue(name);
        }

        @Override
        public void sendMessage(Id target, Value<?> value) {
            // TODO: implement
            throw new ComputerException("Not implemented");
        }

        @Override
        public void sendMessageToAllEdges(Vertex vertex, Value<?> value) {
            // TODO: implement
            throw new ComputerException("Not implemented");
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
        public <V extends Value<?>> Combiner<V> combiner() {
            return (Combiner<V>) WorkerService.this.combiner;
        }
    }
}
