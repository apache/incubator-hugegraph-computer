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

package com.baidu.hugegraph.computer.core.master;

import java.net.InetSocketAddress;
import java.util.List;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.aggregator.Aggregator;
import com.baidu.hugegraph.computer.core.aggregator.MasterAggrManager;
import com.baidu.hugegraph.computer.core.bsp.Bsp4Master;
import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.ContainerInfo;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.SuperstepStat;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.input.MasterInputManager;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.rpc.MasterRpcManager;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

/**
 * Master service is job's controller. It controls the superstep iteration of
 * the job. Master service assembles the managers used by master. For example,
 * aggregator manager, input manager and so on.
 */
public class MasterService {

    private static final Logger LOG = Log.logger(MasterService.class);

    private final Managers managers;

    private boolean inited;
    private Config config;
    private Bsp4Master bsp4Master;
    private ContainerInfo masterInfo;
    private List<ContainerInfo> workers;
    private int maxSuperStep;
    private MasterComputation masterComputation;

    public MasterService() {
        this.managers = new Managers();
    }

    /**
     * Init master service, create the managers used by master.
     */
    public void init(Config config) {
        E.checkArgument(!this.inited, "The %s has been initialized", this);

        this.config = config;

        this.maxSuperStep = this.config.get(ComputerOptions.BSP_MAX_SUPER_STEP);

        this.bsp4Master = new Bsp4Master(this.config);
        this.bsp4Master.init();

        InetSocketAddress rpcAddress = this.initManagers();

        this.masterInfo = new ContainerInfo(ContainerInfo.MASTER_ID,
                                            rpcAddress.getHostName(),
                                            rpcAddress.getPort());

        this.masterComputation = this.config.createObject(
                                 ComputerOptions.MASTER_COMPUTATION_CLASS);
        // TODO: pass aggregator4Master to init
        this.masterComputation.init(this.config, null);

        this.bsp4Master.registerMaster(this.masterInfo);
        this.workers = this.bsp4Master.waitWorkersRegistered();
        LOG.info("{} MasterService worker count: {}",
                 this, this.workers.size());
        LOG.info("{} MasterService initialized", this);
        this.inited = true;
    }

    /**
     * Stop the the master service. Stop the managers created in
     * {@link #init(Config)}.
     */
    public void close() {
        this.checkInited();

        this.managers.closeAll(this.config);

        this.bsp4Master.clean();
        this.bsp4Master.close();
        LOG.info("{} MasterService closed", this);
    }

    /**
     * Execute the graph. First determines which superstep to start from. And
     * then execute the superstep iteration.
     * After the superstep iteration, output the result.
     */
    public void execute() {
        this.checkInited();

        LOG.info("{} MasterService execute", this);
        /*
         * Step 1: Determines which superstep to start from, and resume this
         * superstep.
         */
        int superstep = this.superstepToResume();

        LOG.info("{} MasterService resume from superstep: {}",
                 this, superstep);
        /*
         * TODO: Get input splits from HugeGraph if resume from
         * Constants.INPUT_SUPERSTEP.
         */
        this.bsp4Master.masterSuperstepResume(superstep);

        /*
         * Step 2: Input superstep for loading vertices and edges.
         * This step may be skipped if resume from other superstep than
         * Constants.INPUT_SUPERSTEP.
         */
        SuperstepStat superstepStat;
        if (superstep == Constants.INPUT_SUPERSTEP) {
            superstepStat = this.inputstep();
            superstep++;
        } else {
            // TODO: Get superstepStat from bsp service.
            superstepStat = null;
        }
        E.checkState(superstep <= this.maxSuperStep,
                     "The superstep {} > maxSuperStep {}",
                     superstep, this.maxSuperStep);
        // Step 3: Iteration computation of all supersteps.
        for (; superstepStat.active(); superstep++) {
            LOG.info("{} MasterService superstep {} started",
                     this, superstep);
            /*
             * Superstep iteration. The steps in each superstep are:
             * 1) Master waits workers superstep prepared.
             * 2) All managers call beforeSuperstep.
             * 3) Master signals the workers that the master prepared
             *    superstep.
             * 4) Master waits the workers do vertex computation, and get
             *    superstepStat.
             * 5) Master compute whether to continue the next superstep
             *    iteration.
             * 6) All managers call afterSuperstep.
             * 7) Master signals the workers with superstepStat, and workers
             *    know whether to continue the next superstep iteration.
             */
            this.bsp4Master.waitWorkersSuperstepPrepared(superstep);
            this.managers.beforeSuperstep(this.config, superstep);
            this.bsp4Master.masterSuperstepPrepared(superstep);
            List<WorkerStat> workerStats =
                    this.bsp4Master.waitWorkersSuperstepDone(superstep);
            superstepStat = SuperstepStat.from(workerStats);
            SuperstepContext context = new SuperstepContext(superstep,
                                                            superstepStat);
            boolean masterContinue = this.masterComputation.compute(context);
            if (this.finishedIteration(masterContinue, context)) {
                superstepStat.inactivate();
            }
            this.managers.afterSuperstep(this.config, superstep);
            this.bsp4Master.masterSuperstepDone(superstep, superstepStat);
            LOG.info("{} MasterService superstep {} finished",
                     this, superstep);
        }

        // Step 4: Output superstep for outputting results.
        this.outputstep();
    }

    @Override
    public String toString() {
        return String.format("[master %s]", this.masterInfo.id());
    }

    private InetSocketAddress initManagers() {
        // Create managers
        MasterInputManager inputManager = new MasterInputManager();
        this.managers.add(inputManager);

        MasterAggrManager aggregatorManager = this.config.createObject(
                          ComputerOptions.MASTER_AGGREGATOR_MANAGER_CLASS);
        this.managers.add(aggregatorManager);

        MasterRpcManager rpcManager = new MasterRpcManager();
        this.managers.add(rpcManager);

        // Init managers
        this.managers.initAll(this.config);

        // Register rpc service
        rpcManager.registerInputSplitService(inputManager.handler());
        rpcManager.registerAggregatorService(aggregatorManager.handler());

        // Start rpc server
        InetSocketAddress address = rpcManager.start();
        LOG.info("{} MasterService started rpc server: {}", this, address);
        return address;
    }

    private void checkInited() {
        E.checkArgument(this.inited, "The %s has not been initialized", this);
    }

    private int superstepToResume() {
        /*
         * TODO: determines which superstep to start from if failover is
         * enabled.
         */
        return Constants.INPUT_SUPERSTEP;
    }

    /**
     * The superstep iteration stops if met one of the following conditions:
     * 1): Has run maxSuperStep times of superstep iteration.
     * 2): The mater-computation returns false that stop superstep iteration.
     * 3): All vertices are inactive and no message sent in a superstep.
     * @param masterContinue The master-computation decide
     * @return true if finish superstep iteration.
     */
    private boolean finishedIteration(boolean masterContinue,
                                      MasterContext context) {
        if (!masterContinue) {
            return true;
        }
        if (context.superstep() == this.maxSuperStep - 1) {
            return true;
        }
        long notFinishedVertexCount = context.totalVertexCount() -
                                      context.finishedVertexCount();
        return context.messageCount() == 0L && notFinishedVertexCount == 0L;
    }

    /**
     * Coordinate with workers to load vertices and edges from HugeGraph. There
     * are two phases in inputstep. First phase is get input splits from
     * master, and read the vertices and edges from input splits. The second
     * phase is after all workers read input splits, the workers merge the
     * vertices and edges to get the stats for each partition.
     */
    private SuperstepStat inputstep() {
        LOG.info("{} MasterService inputstep started", this);
        this.bsp4Master.waitWorkersInputDone();
        this.bsp4Master.masterInputDone();
        List<WorkerStat> workerStats = this.bsp4Master.waitWorkersSuperstepDone(
                                       Constants.INPUT_SUPERSTEP);
        SuperstepStat superstepStat = SuperstepStat.from(workerStats);
        this.bsp4Master.masterSuperstepDone(Constants.INPUT_SUPERSTEP,
                                            superstepStat);
        LOG.info("{} MasterService inputstep finished", this);
        return superstepStat;
    }

    /**
     * Wait the workers write result back. After this, the job is finished
     * successfully.
     */
    private void outputstep() {
        LOG.info("{} MasterService outputstep started", this);
        this.bsp4Master.waitWorkersOutputDone();
        LOG.info("{} MasterService outputstep finished", this);
    }

    private static class SuperstepContext implements MasterContext {

        private final int superstep;
        private final SuperstepStat superstepStat;

        public SuperstepContext(int superstep, SuperstepStat superstepStat) {
            this.superstep = superstep;
            this.superstepStat = superstepStat;
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
        public long finishedVertexCount() {
            return this.superstepStat.finishedVertexCount();
        }

        @Override
        public long messageCount() {
            return this.superstepStat.messageCount();
        }

        @Override
        public long messageBytes() {
            return this.superstepStat.messageBytes();
        }

        @Override
        public int superstep() {
            return this.superstep;
        }

        @Override
        public void registerAggregator(
                    String name,
                    Class<? extends Aggregator<Value<?>>> aggregator) {
            // TODO: implement
            throw new ComputerException("Not implemented");
        }

        @Override
        public void registerAggregator(
                    String name, ValueType type,
                    Class<? extends Combiner<Value<?>>> combiner) {
            // TODO: implement
            throw new ComputerException("Not implemented");
        }

        @Override
        public <V extends Value<?>> void aggregatedValue(String name, V value) {
            // TODO: implement
            throw new ComputerException("Not implemented");
        }

        @Override
        public <V extends Value<?>> V aggregatedValue(String name) {
            // TODO: implement
            throw new ComputerException("Not implemented");
        }
    }
}
