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

public class Bsp4Master extends BspBase {

    private static final Logger LOG = Log.logger(Bsp4Master.class);

    public Bsp4Master(Config config) {
        super(config);
    }

    /**
     * Register Master, workers can get master information.
     */
    public void masterInitDone(ContainerInfo masterInfo) {
        String path = this.constructPath(BspEvent.BSP_MASTER_INIT_DONE);
        this.bspClient().put(path, SerializeUtil.toBytes(masterInfo));
        LOG.info("Master is registered, master info: {}", masterInfo);
    }

    /**
     * Wait workers registered.
     */
    public List<ContainerInfo> waitWorkersInitDone() {
        LOG.info("Master is waiting for workers registered");
        String path = this.constructPath(BspEvent.BSP_WORKER_INIT_DONE);
        List<byte[]> serializedContainers = this.waitOnWorkersEvent(
                                            path, this.registerTimeout());
        List<ContainerInfo> containers = new ArrayList<>(this.workerCount());
        for (byte[] serializedContainer : serializedContainers) {
            ContainerInfo container = new ContainerInfo();
            SerializeUtil.fromBytes(serializedContainer, container);
            containers.add(container);
        }
        LOG.info("Master waited all workers init done, workers: {}",
                 containers);
        this.assignContainerId(containers);
        this.masterAllRegisterDone(containers);
        return containers;
    }

    /**
     * The master determines which superstep to start from
     */
    public void masterResume(int superstep) {
        String path = this.constructPath(BspEvent.BSP_MASTER_RESUME);
        IntValue superstepWritable = new IntValue(superstep);
        this.bspClient().put(path, SerializeUtil.toBytes(superstepWritable));
        LOG.info("Master set superstep-resume({})", superstep);
    }

    /**
     * Wait all workers read input splits, and send all vertices and
     * edges to correspond workers. After this, master call masterInputDone.
     */
    public void waitWorkersInputDone() {
        LOG.info("Master is waiting for workers input-done");
        String path = this.constructPath(BspEvent.BSP_WORKER_INPUT_DONE);
        this.waitOnWorkersEvent(path, this.barrierOnWorkersTimeout());
        LOG.info("Master waited workers input-done");
    }

    /**
     * The master signal workers the master input done, the workers can merge
     * vertices and edges after receive this signal.
     */
    public void masterInputDone() {
        LOG.info("Master set input-done");
        String path = this.constructPath(BspEvent.BSP_MASTER_INPUT_DONE);
        this.bspClient().put(path, Constants.EMPTY_BYTES);
    }

    /**
     * Wait workers finish specified superstep. The master receives the
     * worker stat from all workers, calls algorithm's master computation,
     * check the max iteration count, and then calls masterSuperstepDone to
     * synchronize superstep result.
     */
    public List<WorkerStat> waitWorkersStepDone(int superstep) {
        LOG.info("Master is waiting for workers superstep-done({})", superstep);
        String path = this.constructPath(BspEvent.BSP_WORKER_STEP_DONE,
                                         superstep);
        List<byte[]> list = this.waitOnWorkersEvent(path,
                            this.barrierOnWorkersTimeout());
        List<WorkerStat> result = new ArrayList<>(this.workerCount());
        for (byte[] bytes : list) {
            WorkerStat workerStat = new WorkerStat();
            SerializeUtil.fromBytes(bytes, workerStat);
            result.add(workerStat);
        }
        LOG.info("Master waited workers superstep-done({}), workers stat: {}",
                 superstep, result);
        return result;
    }

    /**
     * After all workers prepared superstep, master prepare superstep, and
     * call masterPrepareSuperstepDone to let the workers know that master is
     * prepared done.
     */
    public void waitWorkersStepPrepareDone(int superstep) {
        LOG.info("Master is waiting for workers superstep-prepared({})",
                 superstep);
        String path = this.constructPath(BspEvent.BSP_WORKER_STEP_PREPARE_DONE,
                                         superstep);
        this.waitOnWorkersEvent(path, this.barrierOnWorkersTimeout());
        LOG.info("Master waited workers superstep-prepared");
    }

    /**
     * Master signals the workers that the master superstep prepared.
     */
    public void masterStepPrepareDone(int superstep) {
        LOG.info("Master set superstep-prepared({})", superstep);
        String path = this.constructPath(BspEvent.BSP_MASTER_STEP_PREPARE_DONE,
                                         superstep);
        this.bspClient().put(path, Constants.EMPTY_BYTES);
    }

    /**
     * Master signals the workers that superstep done. The workers read
     * GraphStat and determines whether to continue iteration.
     */
    public void masterStepDone(int superstep,
                               SuperstepStat superstepStat) {
        String path = this.constructPath(BspEvent.BSP_MASTER_STEP_DONE,
                                         superstep);
        this.bspClient().put(path, SerializeUtil.toBytes(superstepStat));
        LOG.info("Master set superstep-done({}), graph stat: {}",
                 superstep, superstepStat);
    }

    /**
     * Wait workers output the vertices.
     */
    public void waitWorkersOutputDone() {
        LOG.info("Master is waiting for workers output-done");
        String path = this.constructPath(BspEvent.BSP_WORKER_OUTPUT_DONE);
        this.waitOnWorkersEvent(path, this.barrierOnWorkersTimeout());
        LOG.info("Master waited workers output-done");
    }

    public void clean() {
        this.bspClient().clean();
        LOG.info("Cleaned up the BSP data");
    }

    private List<byte[]> waitOnWorkersEvent(String prefix, long timeout) {
        return this.bspClient().getChildren(prefix, this.workerCount(),
                                            timeout, this.logInterval());
    }

    private void assignContainerId(List<ContainerInfo> containers) {
        // Assign worker id from 1.
        for (int i = 0; i < containers.size(); i++) {
            containers.get(i).id(i + 1);
        }
    }

    private void masterAllRegisterDone(List<ContainerInfo> workers) {
        String path = this.constructPath(BspEvent.BSP_MASTER_ALL_INIT_DONE);
        this.bspClient().put(path, SerializeUtil.toBytes(workers));
        LOG.info("Master all register done, workers {}", workers);
    }
}
