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

public class EtcdBsp4Worker extends EtcdBspBase implements Bsp4Worker {

    private static final Logger LOG = Log.logger(EtcdBsp4Worker.class);

    private final ContainerInfo workerInfo;

    public EtcdBsp4Worker(HugeConfig config, ContainerInfo workerInfo) {
        super(config);
        this.workerInfo = workerInfo;
    }

    @Override
    public void registerWorker() {
        String path = constructPath(BspEvent.BSP_WORKER_REGISTERED,
                                    this.workerInfo.id());
        this.etcdClient.put(path, ReadWriteUtil.toBytes(this.workerInfo));
        LOG.info("Worker {} registered", this.workerInfo);
    }

    @Override
    public ContainerInfo waitMasterRegistered() {
        String path = constructPath(BspEvent.BSP_MASTER_REGISTERED);
        byte[] bytes = this.etcdClient.get(path, this.registerTimeout);
        ContainerInfo masterInfo = new ContainerInfo();
        ReadWriteUtil.fromBytes(bytes, masterInfo);
        LOG.info("Master {} registered", masterInfo);
        return masterInfo;
    }

    public List<ContainerInfo> waitWorkersRegistered() {
        String path = constructPath(BspEvent.BSP_WORKER_REGISTERED);
        List<byte[]> serializedContainers = this.etcdClient.getWithPrefix(
                                                 path, this.workerCount,
                                                 this.registerTimeout,
                                                 this.logInterval);
        List<ContainerInfo> containers = new ArrayList<>(this.workerCount);
        for (byte[] serializedContainer : serializedContainers) {
            ContainerInfo container = new ContainerInfo();
            ReadWriteUtil.fromBytes(serializedContainer, container);
            containers.add(container);
        }
        LOG.info("All workers registered, workers: {}", containers);
        return containers;
    }

    @Override
    public int waitMasterSuperstepResume() {
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_RESUME);
        byte[] bytes = this.etcdClient.get(path, this.barrierOnMasterTimeout);
        IntValue superstep = new IntValue();
        ReadWriteUtil.fromBytes(bytes, superstep);
        LOG.info("Resume from superstep {}", superstep.value());
        return superstep.value();
    }

    @Override
    public void workerInputDone() {
        String path = constructPath(BspEvent.BSP_WORKER_INPUT_DONE,
                                    this.workerInfo.id());
        this.etcdClient.put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker {} input done", this.workerInfo.id());
    }

    @Override
    public void waitMasterInputDone() {
        String path = constructPath(BspEvent.BSP_MASTER_INPUT_DONE);
        this.etcdClient.get(path, this.barrierOnMasterTimeout);
        LOG.info("Master input done");
    }

    @Override
    public void workerSuperstepDone(int superstep, WorkerStat workerStat) {
        String path = constructPath(BspEvent.BSP_WORKER_SUPERSTEP_DONE,
                                    superstep, this.workerInfo.id());
        this.etcdClient.put(path, ReadWriteUtil.toBytes(workerStat));
        LOG.info("Worker superstep {} done, worker stat: {}",
                 superstep, workerStat);
    }

    @Override
    public GraphStat waitMasterSuperstepDone(int superstep) {
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_DONE,
                                    superstep);
        byte[] bytes = this.etcdClient.get(path, this.barrierOnMasterTimeout);
        GraphStat graphStat = new GraphStat();
        ReadWriteUtil.fromBytes(bytes, graphStat);
        LOG.info("Master superstep {} done, graph stat: {}",
                 superstep, graphStat);
        return graphStat;
    }

    @Override
    public void workerSuperstepPrepared(int superstep) {
        String path = constructPath(BspEvent.BSP_WORKER_SUPERSTEP_PREPARED,
                                    superstep, this.workerInfo.id());
        this.etcdClient.put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker {} prepared superstep {} done",
                 this.workerInfo.id(), superstep);
    }

    @Override
    public void waitMasterSuperstepPrepared(int superstep) {
        LOG.info("Waiting master prepared superstep {} done", superstep);
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_PREPARED,
                                    superstep);
        this.etcdClient.get(path, this.barrierOnMasterTimeout);
    }

    @Override
    public void workerOutputDone() {
        String path = constructPath(BspEvent.BSP_WORKER_OUTPUT_DONE,
                                    this.workerInfo.id());
        this.etcdClient.put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker {} output done", this.workerInfo.id());
    }
}
