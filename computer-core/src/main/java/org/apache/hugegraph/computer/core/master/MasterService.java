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

package org.apache.hugegraph.computer.core.master;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hugegraph.computer.core.aggregator.Aggregator;
import org.apache.hugegraph.computer.core.aggregator.DefaultAggregator;
import org.apache.hugegraph.computer.core.aggregator.MasterAggrManager;
import org.apache.hugegraph.computer.core.bsp.Bsp4Master;
import org.apache.hugegraph.computer.core.combiner.Combiner;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.ContainerInfo;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.SuperstepStat;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.input.MasterInputManager;
import org.apache.hugegraph.computer.core.manager.Managers;
import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.computer.core.output.ComputerOutput;
import org.apache.hugegraph.computer.core.rpc.MasterRpcManager;
import org.apache.hugegraph.computer.core.util.ShutdownHook;
import org.apache.hugegraph.computer.core.worker.WorkerStat;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.TimeUtil;
import org.slf4j.Logger;

/**
 * Master service is job's controller. It controls the superstep iteration of
 * the job. Master service assembles the managers used by master. For example,
 * aggregator manager, input manager and so on.
 */
public class MasterService implements Closeable {

    private static final Logger LOG = Log.logger(MasterService.class);

    private final ComputerContext context;
    private final Managers managers;

    private volatile boolean inited;
    private volatile boolean failed;
    private volatile boolean closed;
    private Config config;
    private volatile Bsp4Master bsp4Master;
    private ContainerInfo masterInfo;
    private int maxSuperStep;
    private MasterComputation masterComputation;

    private final ShutdownHook shutdownHook;
    private volatile Thread serviceThread;

    public MasterService() {
        this.context = ComputerContext.instance();
        this.managers = new Managers();
        this.closed = false;
        this.shutdownHook = new ShutdownHook();
    }

    /**
     * Init master service, create the managers used by master.
     */
    public void init(Config config) {
        E.checkArgument(!this.inited, "The %s has been initialized", this);
        LOG.info("{} Start to initialize master", this);

        this.serviceThread = Thread.currentThread();
        this.registerShutdownHook();

        this.config = config;

        this.maxSuperStep = this.config.get(ComputerOptions.BSP_MAX_SUPER_STEP);

        InetSocketAddress rpcAddress = this.initManagers();

        this.masterInfo = new ContainerInfo(ContainerInfo.MASTER_ID,
                                            TransportUtil.host(rpcAddress),
                                            rpcAddress.getPort());
        /*
         * Connect to BSP server and clean the old data may be left by the
         * previous job with the same job id.
         */
        this.bsp4Master = new Bsp4Master(this.config);
        this.bsp4Master.clean();

        this.masterComputation = this.config.createObject(
                                 ComputerOptions.MASTER_COMPUTATION_CLASS);
        this.masterComputation.init(new DefaultMasterContext());
        this.managers.initedAll(config);

        LOG.info("{} register MasterService", this);
        this.bsp4Master.masterInitDone(this.masterInfo);

        List<ContainerInfo> workers = this.bsp4Master.waitWorkersInitDone();
        LOG.info("{} waited all workers registered, workers count: {}",
                 this, workers.size());

        LOG.info("{} MasterService initialized", this);
        this.inited = true;
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

    private void registerShutdownHook() {
        this.shutdownHook.hook(() -> {
            this.stopServiceThread();
            this.cleanAndCloseBsp();
        });
    }

    /**
     * Stop the master service. Stop the managers created in {@link #init(Config)}.
     */
    @Override
    public synchronized void close() {
        // TODO: check the logic of close carefully later
        //this.checkInited();
        if (this.closed) {
            LOG.info("{} MasterService had closed before", this);
            return;
        }

        try {
            if (this.masterComputation != null) {
                this.masterComputation.close(new DefaultMasterContext());
            }
        } catch (Exception e) {
            LOG.error("Error occurred while closing master service", e);
        }

        if (!failed && this.bsp4Master != null) {
            this.bsp4Master.waitWorkersCloseDone();
        }

        try {
            if (managers != null) {
                this.managers.closeAll(this.config);
            }
        } catch (Exception e) {
            LOG.error("Error occurred while closing managers", e);
        }

        this.cleanAndCloseBsp();
        this.shutdownHook.unhook();

        this.closed = true;
        LOG.info("{} MasterService closed", this);
    }

    private void cleanAndCloseBsp() {
        if (this.bsp4Master == null) {
            return;
        }

        this.bsp4Master.clean();
        this.bsp4Master.close();
    }

    /**
     * Execute the graph. First determines which superstep to start from. And
     * then execute the superstep iteration.
     * After the superstep iteration, output the result.
     */
    public void execute() {
        StopWatch watcher = new StopWatch();
        this.checkInited();

        LOG.info("{} MasterService execute", this);
        try {
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
            this.bsp4Master.masterResumeDone(superstep);

            /*
             * Step 2: Input superstep for loading vertices and edges.
             * This step may be skipped if resume from other superstep than
             * Constants.INPUT_SUPERSTEP.
             */
            SuperstepStat superstepStat;
            watcher.start();
            if (superstep == Constants.INPUT_SUPERSTEP) {
                superstepStat = this.inputstep();
                superstep++;
            } else {
                // TODO: Get superstepStat from bsp service.
                superstepStat = null;
            }
            watcher.stop();
            LOG.info("{} MasterService input step cost: {}",
                     this, TimeUtil.readableTime(watcher.getTime()));
            E.checkState(superstep <= this.maxSuperStep,
                         "The superstep {} can't be > maxSuperStep {}",
                         superstep, this.maxSuperStep);

            watcher.reset();
            watcher.start();
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
                 * 4) Master waits the workers do vertex computation.
                 * 5) Master signal the workers that all workers have finished
                 *    vertex computation.
                 * 6) Master waits the workers end the superstep, and get
                 *    superstepStat.
                 * 7) Master compute whether to continue the next superstep
                 *    iteration.
                 * 8) All managers call afterSuperstep.
                 * 9) Master signals the workers with superstepStat, and workers
                 *    know whether to continue the next superstep iteration.
                 */
                this.bsp4Master.waitWorkersStepPrepareDone(superstep);
                this.managers.beforeSuperstep(this.config, superstep);
                this.bsp4Master.masterStepPrepareDone(superstep);

                this.bsp4Master.waitWorkersStepComputeDone(superstep);
                this.bsp4Master.masterStepComputeDone(superstep);
                List<WorkerStat> workerStats =
                        this.bsp4Master.waitWorkersStepDone(superstep);
                superstepStat = SuperstepStat.from(workerStats);
                SuperstepContext context = new SuperstepContext(superstep,
                                                                superstepStat);
                // Call master compute(), note the worker afterSuperstep() is done
                boolean masterContinue = this.masterComputation.compute(context);
                if (this.finishedIteration(masterContinue, context)) {
                    superstepStat.inactivate();
                }
                this.managers.afterSuperstep(this.config, superstep);
                this.bsp4Master.masterStepDone(superstep, superstepStat);

                LOG.info("{} MasterService superstep {} finished",
                         this, superstep);
            }
            watcher.stop();
            LOG.info("{} MasterService compute step cost: {}",
                     this, TimeUtil.readableTime(watcher.getTime()));

            watcher.reset();
            watcher.start();
            // Step 4: Output superstep for outputting results.
            this.outputstep();
            watcher.stop();
            LOG.info("{} MasterService output step cost: {}",
                     this, TimeUtil.readableTime(watcher.getTime()));
        } catch (Throwable throwable) {
            LOG.error("{} MasterService execute failed", this, throwable);
            failed = true;
            throw throwable;
        }
    }

    @Override
    public String toString() {
        Object id = this.masterInfo == null ?
                    "?" + this.hashCode() : this.masterInfo.id();
        return String.format("[master %s]", id);
    }

    private InetSocketAddress initManagers() {
        // Create managers
        MasterInputManager inputManager = new MasterInputManager();
        this.managers.add(inputManager);

        MasterAggrManager aggregatorManager = new MasterAggrManager();
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
     * @param masterContinue The master-computation decides
     * @return true if finish superstep iteration.
     */
    private boolean finishedIteration(boolean masterContinue,
                                      MasterComputationContext context) {
        if (!masterContinue) {
            return true;
        }
        if (context.superstep() >= this.maxSuperStep - 1) {
            return true;
        }
        long activeVertexCount = context.totalVertexCount() -
                                 context.finishedVertexCount();
        return context.messageCount() == 0L && activeVertexCount == 0L;
    }

    /**
     * Coordinate with workers to load vertices and edges from HugeGraph. There
     * are two phases in inputstep. The First phase is to get input splits from
     * master, and read the vertices and edges from input splits. The second
     * phase is after all workers read input splits, the workers merge the
     * vertices and edges to get the stats for each partition.
     */
    private SuperstepStat inputstep() {
        LOG.info("{} MasterService inputstep started", this);
        this.bsp4Master.waitWorkersInputDone();
        this.bsp4Master.masterInputDone();
        List<WorkerStat> workerStats = this.bsp4Master.waitWorkersStepDone(
                                       Constants.INPUT_SUPERSTEP);
        SuperstepStat superstepStat = SuperstepStat.from(workerStats);
        this.bsp4Master.masterStepDone(Constants.INPUT_SUPERSTEP,
                                       superstepStat);
        LOG.info("{} MasterService inputstep finished with superstat {}",
                 this, superstepStat);
        return superstepStat;
    }

    /**
     * Wait the workers write a result back. After this, the job is finished successfully.
     */
    private void outputstep() {
        LOG.info("{} MasterService outputstep started", this);
        this.bsp4Master.waitWorkersOutputDone();
        // Merge output files of multiple partitions
        ComputerOutput output = this.config.createObject(
                                ComputerOptions.OUTPUT_CLASS);
        output.mergePartitions(this.config);
        LOG.info("{} MasterService outputstep finished", this);
    }

    private class DefaultMasterContext implements MasterContext {

        private final MasterAggrManager aggrManager;

        public DefaultMasterContext() {
            this.aggrManager = MasterService.this.managers.get(
                               MasterAggrManager.NAME);
        }

        @Override
        public <V extends Value, C extends Aggregator<V>>
        void registerAggregator(String name, Class<C> aggregatorClass) {
            E.checkArgument(aggregatorClass != null,
                            "The aggregator class can't be null");
            Aggregator<V> aggr;
            try {
                aggr = aggregatorClass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new ComputerException("Can't new instance from class: %s",
                                            e, aggregatorClass.getName());
            }
            this.aggrManager.registerAggregator(name, aggr);
        }

        @Override
        public <V extends Value, C extends Combiner<V>>
        void registerAggregator(String name, ValueType type,
                                Class<C> combinerClass) {
            this.registerAggregator(name, type, combinerClass, null);
        }

        @Override
        public <V extends Value, C extends Combiner<V>>
        void registerAggregator(String name, V defaultValue,
                                Class<C> combinerClass) {
            E.checkArgument(defaultValue != null,
                            "The aggregator default value can't be null: %s," +
                            " or call another register method if necessary: " +
                            "registerAggregator(String name,ValueType type," +
                            "Class<C> combiner)", name);
            this.registerAggregator(name, defaultValue.valueType(),
                                    combinerClass, defaultValue);
        }

        private <V extends Value, C extends Combiner<V>>
        void registerAggregator(String name, ValueType type,
                                Class<C> combinerClass, V defaultValue) {
            Aggregator<V> aggr = new DefaultAggregator<>(
                                 MasterService.this.context,
                                 type, combinerClass, defaultValue);
            this.aggrManager.registerAggregator(name, aggr);
        }

        @Override
        public <V extends Value> void aggregatedValue(String name, V value) {
            this.aggrManager.aggregatedAggregator(name, value);
        }

        @Override
        public <V extends Value> V aggregatedValue(String name) {
            return this.aggrManager.aggregatedValue(name);
        }

        @Override
        public Config config() {
            return MasterService.this.config;
        }
    }

    private class SuperstepContext extends DefaultMasterContext
                                   implements MasterComputationContext {

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
            return this.superstepStat.messageSendCount();
        }

        @Override
        public long messageBytes() {
            return this.superstepStat.messageSendBytes();
        }

        @Override
        public int superstep() {
            return this.superstep;
        }
    }
}
