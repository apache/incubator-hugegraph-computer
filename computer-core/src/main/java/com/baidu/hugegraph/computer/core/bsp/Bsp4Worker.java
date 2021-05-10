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

package com.baidu.hugegraph.computer.core.bsp;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.ContainerInfo;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.SuperstepStat;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.util.SerializeUtil;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.util.Log;

public class Bsp4Worker extends BspBase {

    private static final Logger LOG = Log.logger(Bsp4Worker.class);

    private final ContainerInfo workerInfo;

    public Bsp4Worker(Config config, ContainerInfo workerInfo) {
        super(config);
        this.workerInfo = workerInfo;
    }

    /**
     * Register this worker, worker's information is passed by constructor.
     */
    public void registerWorker() {
        String path = this.constructPath(BspEvent.BSP_WORKER_REGISTERED,
                                         this.workerInfo.id());
        this.bspClient().put(path, SerializeUtil.toBytes(this.workerInfo));
        LOG.info("Worker is registered: {}", this.workerInfo);
    }

    /**
     * Wait master registered, get master's information includes hostname
     * and port.
     */
    public ContainerInfo waitMasterRegistered() {
        LOG.info("Worker({}) is waiting for master registered",
                 this.workerInfo.id());
        String path = this.constructPath(BspEvent.BSP_MASTER_REGISTERED);
        byte[] bytes = this.bspClient().get(path, this.registerTimeout(),
                                            this.logInterval());
        ContainerInfo masterInfo = new ContainerInfo();
        SerializeUtil.fromBytes(bytes, masterInfo);
        LOG.info("Worker({}) waited master registered: {}",
                 this.workerInfo.id(), masterInfo);
        return masterInfo;
    }

    /**
     * Get all workers information includes hostname and port the workers
     * listen on.
     */
    public List<ContainerInfo> waitWorkersRegistered() {
        LOG.info("Worker({}) is waiting for master all-registered",
                 this.workerInfo.id());
        // TODO: change to wait BSP_MASTER_ALL_REGISTERED
        String path = this.constructPath(BspEvent.BSP_WORKER_REGISTERED);
        List<byte[]> serializedContainers = this.bspClient().getChildren(
                                            path, this.workerCount(),
                                            this.registerTimeout(),
                                            this.logInterval());
        List<ContainerInfo> containers = new ArrayList<>(this.workerCount());
        for (byte[] serializedContainer : serializedContainers) {
            ContainerInfo container = new ContainerInfo();
            SerializeUtil.fromBytes(serializedContainer, container);
            containers.add(container);
        }
        LOG.info("Worker({}) waited master all-registered, workers: {}",
                 this.workerInfo.id(), containers);
        return containers;
    }

    /**
     * The master set this signal to let workers knows the first superstep to
     * start with.
     */
    public int waitMasterSuperstepResume() {
        LOG.info("Worker({}) is waiting for master superstep-resume",
                 this.workerInfo.id());
        String path = this.constructPath(BspEvent.BSP_MASTER_SUPERSTEP_RESUME);
        byte[] bytes = this.bspClient().get(path, this.barrierOnMasterTimeout(),
                                            this.logInterval());
        IntValue superstep = new IntValue();
        SerializeUtil.fromBytes(bytes, superstep);
        LOG.info("Worker({}) waited superstep-resume({})",
                 this.workerInfo.id(), superstep.value());
        return superstep.value();
    }

    /**
     * Set read done signal after read input splits, and send all vertices and
     * edges to correspond workers.
     */
    public void workerInputDone() {
        String path = this.constructPath(BspEvent.BSP_WORKER_INPUT_DONE,
                                         this.workerInfo.id());
        this.bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker({}) set input-done", this.workerInfo.id());
    }

    /**
     * Wait master signal that all workers input done. After this, worker
     * can merge the vertices and edges.
     */
    public void waitMasterInputDone() {
        LOG.info("Worker({}) is waiting for master input-done",
                 this.workerInfo.id());
        String path = this.constructPath(BspEvent.BSP_MASTER_INPUT_DONE);
        this.bspClient().get(path, this.barrierOnMasterTimeout(),
                             this.logInterval());
        LOG.info("Worker({}) waited master input-done", this.workerInfo.id());
    }

    /**
     * Worker set this signal to indicate the worker is ready to receive
     * messages from other workers.
     */
    public void workerSuperstepPrepared(int superstep) {
        String path = this.constructPath(BspEvent.BSP_WORKER_SUPERSTEP_PREPARED,
                                         superstep, this.workerInfo.id());
        this.bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker({}) set superstep-prepared({})",
                 this.workerInfo.id(), superstep);
    }

    /**
     * After receive this signal, the worker can execute and send messages
     * to other workers.
     */
    public void waitMasterSuperstepPrepared(int superstep) {
        LOG.info("Worker({}) is waiting for master superstep-prepared({})",
                 this.workerInfo.id(), superstep);
        String path = this.constructPath(BspEvent.BSP_MASTER_SUPERSTEP_PREPARED,
                                         superstep);
        this.bspClient().get(path, this.barrierOnMasterTimeout(),
                             this.logInterval());
        LOG.info("Worker({}) waited master superstep-prepared({})",
                 this.workerInfo.id(), superstep);
    }

    /**
     * Worker set this signal after sent all messages to corresponding
     * workers and sent aggregators to master.
     */
    public void workerSuperstepDone(int superstep, WorkerStat workerStat) {
        String path = this.constructPath(BspEvent.BSP_WORKER_SUPERSTEP_DONE,
                                         superstep, this.workerInfo.id());
        this.bspClient().put(path, SerializeUtil.toBytes(workerStat));
        LOG.info("Worker({}) set superstep-done({}), worker stat: {}",
                 this.workerInfo.id(), superstep, workerStat);
    }

    /**
     * The master set this signal after all workers signaled superstepDone,
     * and master computes MasterComputation, and broadcast all aggregators to
     * works.
     */
    public SuperstepStat waitMasterSuperstepDone(int superstep) {
        LOG.info("Worker({}) is waiting for master superstep-done({})",
                 this.workerInfo.id(), superstep);
        String path = this.constructPath(BspEvent.BSP_MASTER_SUPERSTEP_DONE,
                                         superstep);
        byte[] bytes = this.bspClient().get(path, this.barrierOnMasterTimeout(),
                                            this.logInterval());
        SuperstepStat superstepStat = new SuperstepStat();
        SerializeUtil.fromBytes(bytes, superstepStat);
        LOG.info("Worker({}) waited master superstep-done({}), graph stat: {}",
                 this.workerInfo.id(), superstep, superstepStat);
        return superstepStat;
    }

    /**
     * Worker set this signal to indicate the worker has outputted the result.
     * It can successfully exit.
     */
    public void workerOutputDone() {
        String path = this.constructPath(BspEvent.BSP_WORKER_OUTPUT_DONE,
                                         this.workerInfo.id());
        this.bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker({}) set output-done", this.workerInfo.id());
    }
}
