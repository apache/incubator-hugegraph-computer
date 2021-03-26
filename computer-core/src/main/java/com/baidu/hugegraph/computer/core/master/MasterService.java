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

import java.util.List;

import com.baidu.hugegraph.computer.core.aggregator.Aggregator;
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
import com.baidu.hugegraph.computer.core.worker.WorkerStat;

/**
 * Master service is job's controller. It controls the superstep iteration of
 * the job. Master service assembles the managers used by master. For example,
 * aggregator manager, input manager and so on.
 */
public class MasterService implements MasterContext {

    private Config config;
    private Bsp4Master bsp4Master;
    private ContainerInfo masterInfo;
    private List<ContainerInfo> workers;
    private SuperstepStat superstepStat;
    private int superstep;
    private int maxSuperStep;

    public MasterService(Config config) {
        this.config = config;
    }

    /**
     * Init master service, create the managers used by master.
     */
    public void init() {
        this.maxSuperStep = this.config.get(ComputerOptions.BSP_MAX_SUPER_STEP);
        /*
         * TODO: start rpc server and get rpc port.
         * TODO: start aggregator manager for master.
         */
        this.bsp4Master = new Bsp4Master(this.config);
        this.bsp4Master.init();
        // TODO: get hostname
        this.masterInfo = new ContainerInfo(-1, "localhost", 8001, 8002);
        this.bsp4Master.registerMaster(this.masterInfo);
        this.workers = this.bsp4Master.waitWorkersRegistered();
    }

    /**
     * Stop the the master service. Stop the managers created in
     * {@link #init()}.
     */
    public void close() {
        this.bsp4Master.clean();
        this.bsp4Master.close();
    }

    /**
     * Execute the graph. First determines which superstep to start from. And
     * then execute the superstep iteration.
     * The superstep iteration stops if met one of the following conditions:
     * 1): Has run maxSuperStep times of superstep iteration.
     * 2): The mater-computation returns false that stop superstep iteration.
     * 3): All vertices are inactive and no message sent in a superstep.
     * After the superstep iteration, output the result.
     */
    public void execute() {
        // TODO: determine superstep if fail over is enabled.
        this.superstep = Constants.INPUT_SUPERSTEP;
        this.bsp4Master.masterSuperstepResume(this.superstep);
        this.input();
        this.superstep++;
        for (; this.superstep < this.maxSuperStep; this.superstep++) {
            this.bsp4Master.waitWorkersSuperstepPrepared(this.superstep);
            this.bsp4Master.masterSuperstepPrepared(this.superstep);
            List<WorkerStat> workerStats =
                             this.bsp4Master.waitWorkersSuperstepDone(
                                             this.superstep);
            this.superstepStat = superstepStat(workerStats);
            // TODO: calls algorithm's master-computation to decide whether
            //  stop iteration
            boolean allVertexInactiveAndNoMessage =
                    this.finishedVertexCount() == this.totalVertexCount() &&
                    this.messageCount() == 0;
            if (this.superstep == this.maxSuperStep - 1 ||
                allVertexInactiveAndNoMessage) {
                this.superstepStat.inactivate();
            }
            this.bsp4Master.masterSuperstepDone(this.superstep,
                                                this.superstepStat);
        }
        this.output();
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
    public <V extends Value> void registerAggregator(
                             String name,
                             Class<? extends Aggregator<V>> aggregatorClass) {
        throw new ComputerException("Not implemented");
    }

    @Override
    public <V extends Value> void registerAggregator(
                             String name,
                             ValueType type,
                             Class<? extends Combiner<V>> combinerClass) {
        throw new ComputerException("Not implemented");
    }

    @Override
    public <V extends Value> void aggregatedValue(String name, V value) {
        throw new ComputerException("Not implemented");
    }

    @Override
    public <V extends Value> V aggregatedValue(String name) {
        throw new ComputerException("Not implemented");
    }

    /**
     * Generate SuperstepStat from workerStat list.
     */
    private static SuperstepStat superstepStat(List<WorkerStat> workerStats) {
        SuperstepStat superstepStat = new SuperstepStat();
        for (WorkerStat workerStat : workerStats) {
            superstepStat.increase(workerStat);
        }
        return superstepStat;
    }

    private void input() {
        this.bsp4Master.waitWorkersInputDone();
        this.bsp4Master.masterInputDone();
        List<WorkerStat> workerStats = this.bsp4Master.waitWorkersSuperstepDone(
                                            Constants.INPUT_SUPERSTEP);
        this.superstepStat = superstepStat(workerStats);
        this.bsp4Master.masterSuperstepDone(Constants.INPUT_SUPERSTEP,
                                            this.superstepStat);
    }

    private void output() {
        this.bsp4Master.waitWorkersOutputDone();
    }
}
