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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.bsp.Bsp4Worker;
import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.ContainerInfo;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.SuperstepStat;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.util.Log;

public class WorkerService implements WorkerContext {

    private static final Logger LOG = Log.logger(WorkerService.class);

    private Config config;
    private Bsp4Worker bsp4Worker;
    private ContainerInfo workerInfo;
    private ContainerInfo masterInfo;
    private Map<Integer, ContainerInfo> workers;
    private int superstep;
    private SuperstepStat superstepStat;
    public List<Manager> managers;

    public WorkerService() {
        this.workers = new HashMap<>();
        this.managers = new ArrayList();
    }

    /**
     * Init worker service, create the managers used by worker service.
     */
    public void init(Config config) {
        this.config = config;
        // TODO: Start data-transport server and get its host and port.
        this.workerInfo = new ContainerInfo(0, "localhost", 0, 8004);
        this.bsp4Worker = new Bsp4Worker(this.config, this.workerInfo);
        this.bsp4Worker.init();
        this.bsp4Worker.registerWorker();
        this.masterInfo = this.bsp4Worker.waitMasterRegistered();
        List<ContainerInfo> containers =
                            this.bsp4Worker.waitWorkersRegistered();
        for (ContainerInfo container : containers) {
            this.workers.put(container.id(), container);
        }
        // TODO: create connections to other workers for data transportation.
        // TODO: create aggregator manager
        LOG.info("WorkerService {} initialized.", this.workerInfo.id());
    }

    /**
     * Stop the worker service. Stop the managers created in
     * {@link #init(Config)}.
     */
    public void close() {
        /*
         * TODO: close the connection to other workers.
         * TODO: stop the connection to the master
         * TODO: stop the data transportation server.
         */
        for (Manager manager : this.managers) {
            manager.close(this.config);
        }
        this.bsp4Worker.close();
        LOG.info("WorkerService {} closed.", this.workerInfo.id());
    }

    /**
     * Execute the superstep in worker. It first wait master witch superstep
     * to start from. And then do the superstep iteration until master's
     * superstepStat is inactive.
     */
    public void execute() {
        LOG.info("WorkerService {} execute.", this.workerInfo.id());
        // TODO: determine superstep if fail over is enabled.
        this.superstep = this.bsp4Worker.waitMasterSuperstepResume();
        if (this.superstep == Constants.INPUT_SUPERSTEP) {
            this.inputstep();
            this.superstep++;
        }
        /*
         * The master determine whether to execute the next superstep. The
         * superstepStat is active while master decides to execute the next
         * superstep.
         */
        while (this.superstepStat.active()) {
            LOG.info("Start iteration of superstep {}", this.superstep);
            for (Manager manager : this.managers) {
                manager.beforeSuperstep(this.config, this.superstep);
            }
            this.bsp4Worker.workerSuperstepPrepared(this.superstep);
            this.bsp4Worker.waitMasterSuperstepPrepared(this.superstep);
            WorkerStat workerStat = this.computePartitions();
            for (Manager manager : this.managers) {
                manager.afterSuperstep(this.config, this.superstep);
            }
            this.bsp4Worker.workerSuperstepDone(this.superstep, workerStat);
            LOG.info("End iteration of superstep {}", this.superstep);
            this.superstepStat = this.bsp4Worker.waitMasterSuperstepDone(
                                                 this.superstep);
            this.superstep++;
        }
        this.outputstep();
    }

    @Override
    public Config config() {
        return this.config;
    }

    @Override
    public <V extends Value> void aggregateValue(String name, V value) {
        throw new ComputerException("Not implemented");
    }

    @Override
    public <V extends Value> V aggregatedValue(String name) {
       throw new ComputerException("Not implemented");
    }

    @Override
    public void sendMessage(Id target, Value value) {
        throw new ComputerException("Not implemented");
    }

    @Override
    public void sendMessageToAllEdges(Vertex vertex, Value value) {
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
    public <V extends Value> Combiner<V> combiner() {
        throw new ComputerException("Not implemented");
    }

    /**
     * Load vertices and edges from HugeGraph. There are two phases in
     * inputstep. First phase is get input splits from master, and read the
     * vertices and edges from input splits. The second phase is after all
     * workers read input splits, the workers merge the vertices and edges to
     * get the stats for each partition.
     */
    private void inputstep() {
        LOG.info("WorkerService {} inputstep started.", this.workerInfo.id());
        /*
         * Load vertices and edges parallel.
         */
        // TODO: calls LoadService to load vertices and edges parallel
        this.bsp4Worker.workerInputDone();
        this.bsp4Worker.waitMasterInputDone();

        /*
         * Merge vertices and edges in partitions parallel, and get workerStat.
         */
        // TODO: merge the data in partitions parallel, and get workerStat
        WorkerStat workerStat = this.computePartitions();
        this.bsp4Worker.workerSuperstepDone(Constants.INPUT_SUPERSTEP,
                                            workerStat);
        this.superstepStat = this.bsp4Worker.waitMasterSuperstepDone(
                             this.superstep);
        LOG.info("WorkerService {} inputstep finished.", this.workerInfo.id());
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
        LOG.info("WorkerService {} outputstep finished.", this.workerInfo.id());
    }

    /**
     * Compute all partitions parallel in this worker. Be called one time for
     * a superstep.
     * @return WorkerStat
     */
    protected WorkerStat computePartitions() {
        // TODO: compute partitions parallel and get workerStat
        throw new ComputerException("Not implemented");
    }
}
