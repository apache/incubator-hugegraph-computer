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
import com.baidu.hugegraph.computer.core.graph.GraphStat;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.util.ReadWriteUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public class Bsp4Worker extends BspBase {

    private static final Logger LOG = Log.logger(Bsp4Worker.class);

    private final ContainerInfo workerInfo;

    public Bsp4Worker(HugeConfig config, ContainerInfo workerInfo) {
        super(config);
        this.workerInfo = workerInfo;
    }

    /**
     * Register this worker, worker's information is passed by constructor.
     */
    public void registerWorker() {
        String path = constructPath(BspEvent.BSP_WORKER_REGISTERED,
                                    this.workerInfo.id());
        bspClient().put(path, ReadWriteUtil.toBytes(this.workerInfo));
        LOG.info("Worker {} registered", this.workerInfo);
    }

    /**
     * Wait master registered, get master's information includes hostname
     * and port.
     */
    public ContainerInfo waitMasterRegistered() {
        String path = constructPath(BspEvent.BSP_MASTER_REGISTERED);
        byte[] bytes = bspClient().get(path, registerTimeout());
        ContainerInfo masterInfo = new ContainerInfo();
        ReadWriteUtil.fromBytes(bytes, masterInfo);
        LOG.info("Master {} registered", masterInfo);
        return masterInfo;
    }

    /**
     * Get all workers information includes hostname and port the workers
     * listen on.
     */
    public List<ContainerInfo> waitWorkersRegistered() {
        String path = constructPath(BspEvent.BSP_WORKER_REGISTERED);
        List<byte[]> serializedContainers = bspClient().getChildren(
                                            path, workerCount(),
                                            registerTimeout(), logInterval());
        List<ContainerInfo> containers = new ArrayList<>(workerCount());
        for (byte[] serializedContainer : serializedContainers) {
            ContainerInfo container = new ContainerInfo();
            ReadWriteUtil.fromBytes(serializedContainer, container);
            containers.add(container);
        }
        LOG.info("All workers registered, workers: {}", containers);
        return containers;
    }

    /**
     * The master set this signal to let workers knows the first superstep to
     * start with.
     */
    public int waitMasterSuperstepResume() {
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_RESUME);
        byte[] bytes = bspClient().get(path, barrierOnMasterTimeout());
        IntValue superstep = new IntValue();
        ReadWriteUtil.fromBytes(bytes, superstep);
        LOG.info("Resume from superstep {}", superstep.value());
        return superstep.value();
    }

    /**
     * Set read done signal after read input splits, and send all vertices and
     * edges to correspond workers.
     */
    public void workerInputDone() {
        String path = constructPath(BspEvent.BSP_WORKER_INPUT_DONE,
                                    this.workerInfo.id());
        bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker {} input done", this.workerInfo.id());
    }

    /**
     * Wait master signal that all workers input done. After this, worker
     * can merge the vertices and edges.
     */
    public void waitMasterInputDone() {
        String path = constructPath(BspEvent.BSP_MASTER_INPUT_DONE);
        bspClient().get(path, barrierOnMasterTimeout());
        LOG.info("Master input done");
    }

    /**
     * Worker set this signal to indicate the worker is ready to receive
     * messages from other workers.
     */
    public void workerSuperstepPrepared(int superstep) {
        String path = constructPath(BspEvent.BSP_WORKER_SUPERSTEP_PREPARED,
                                    superstep, this.workerInfo.id());
        bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker {} prepared superstep {} done",
                 this.workerInfo.id(), superstep);
    }

    /**
     * After receive this signal, the worker can execute and send messages
     * to other workers.
     */
    public void waitMasterSuperstepPrepared(int superstep) {
        LOG.info("Waiting master prepared superstep {} done", superstep);
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_PREPARED,
                                    superstep);
        bspClient().get(path, barrierOnMasterTimeout());
    }

    /**
     * Worker set this signal after sent all messages to corresponding
     * workers and sent aggregators to master.
     */
    public void workerSuperstepDone(int superstep, WorkerStat workerStat) {
        String path = constructPath(BspEvent.BSP_WORKER_SUPERSTEP_DONE,
                                    superstep, this.workerInfo.id());
        bspClient().put(path, ReadWriteUtil.toBytes(workerStat));
        LOG.info("Worker superstep {} done, worker stat: {}",
                 superstep, workerStat);
    }

    /**
     * The master set this signal after all workers signaled superstepDone,
     * and master computes MasterComputation, and broadcast all aggregators to
     * works.
     */
    public GraphStat waitMasterSuperstepDone(int superstep) {
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_DONE,
                                    superstep);
        byte[] bytes = bspClient().get(path, barrierOnMasterTimeout());
        GraphStat graphStat = new GraphStat();
        ReadWriteUtil.fromBytes(bytes, graphStat);
        LOG.info("Master superstep {} done, graph stat: {}",
                 superstep, graphStat);
        return graphStat;
    }

    /**
     * Worker set this signal to indicate the worker has outputted the result.
     * It can successfully exit.
     */
    public void workerOutputDone() {
        String path = constructPath(BspEvent.BSP_WORKER_OUTPUT_DONE,
                                    this.workerInfo.id());
        bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker {} output done", this.workerInfo.id());
    }
}
