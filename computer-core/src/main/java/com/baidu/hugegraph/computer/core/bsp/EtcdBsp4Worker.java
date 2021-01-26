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

    private ContainerInfo workerInfo;

    public EtcdBsp4Worker(HugeConfig config, ContainerInfo workerInfo) {
        super(config);
        this.workerInfo = workerInfo;
    }

    @Override
    public void registerWorker() {
        String path = constructPath(Constants.BSP_WORKER_REGISTER_PATH,
                                    this.workerInfo.id());
        this.etcdClient.put(path, ReadWriteUtil.toByteArray(this.workerInfo));
        LOG.info("Worker {} registered", this.workerInfo);
    }

    @Override
    public ContainerInfo waitMasterRegistered() {
        String path = constructPath(Constants.BSP_MASTER_REGISTER_PATH);
        byte[] bytes = this.etcdClient.get(path, this.registerTimeout, true);
        ContainerInfo masterInfo = new ContainerInfo();
        ReadWriteUtil.readFrom(bytes, masterInfo);
        LOG.info("Master {} registered", masterInfo);
        return masterInfo;
    }

    public List<ContainerInfo> waitWorkersRegistered() {
        String path = constructPath(Constants.BSP_WORKER_REGISTER_PATH);
        List<byte[]> serializedContainers = this.etcdClient.getWithPrefix(
                                                 path, this.workerCount,
                                                 this.registerTimeout, true);
        List<ContainerInfo> containers = new ArrayList<>(this.workerCount);
        for (byte[] serializedContainer : serializedContainers) {
            ContainerInfo container = new ContainerInfo();
            ReadWriteUtil.readFrom(serializedContainer, container);
            containers.add(container);
        }
        LOG.info("All workers registered, workers:{}", containers);
        return containers;
    }

    @Override
    public int waitFirstSuperstep() {
        String path = constructPath(Constants.BSP_MASTER_FIRST_SUPERSTEP_PATH);
        byte[] bytes = this.etcdClient.get(path, this.barrierOnMasterTimeout,
                                           true);
        IntValue superstep = new IntValue();
        ReadWriteUtil.readFrom(bytes, superstep);
        LOG.info("First superstep {}", superstep.value());
        return superstep.value();
    }

    @Override
    public void readDone() {
        String path = constructPath(Constants.BSP_WORKER_READ_DONE_PATH,
                                    this.workerInfo.id());
        this.etcdClient.put(path, Constants.EMPTY_BYTES);
        LOG.info("Read done");
    }

    @Override
    public void waitWorkersReadDone() {
        String path = constructPath(Constants.BSP_WORKER_READ_DONE_PATH);
        this.barrierOnWorkers(path, this.barrierOnWorkersTimeout);
        LOG.info("Workers read done");
    }

    @Override
    public void superstepDone(int superstep, WorkerStat workerStat) {
        String path = constructPath(Constants.BSP_WORKER_SUPERSTEP_DONE_PATH,
                                    superstep, this.workerInfo.id());
        this.etcdClient.put(path, ReadWriteUtil.toByteArray(workerStat));
        LOG.info("Superstep {} done, worker stat:{}", superstep, workerStat);
    }

    @Override
    public GraphStat waitMasterSuperstepDone(int superstep) {
        String path = constructPath(Constants.BSP_MASTER_SUPERSTEP_DONE_PATH,
                                    superstep);
        byte[] bytes = this.etcdClient.get(path, this.barrierOnMasterTimeout,
                                           true);
        GraphStat graphStat = new GraphStat();
        ReadWriteUtil.readFrom(bytes, graphStat);
        LOG.info("Master superstep {} done, graph stat:{}", superstep,
                 graphStat);
        return graphStat;
    }

    @Override
    public void prepareSuperstepDone(int superstep) {
        String path = constructPath(
                      Constants.BSP_WORKER_PREPARE_SUPERSTEP_DONE_PATH,
                      superstep, this.workerInfo.id());
        this.etcdClient.put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker {} prepared superstep {} done",
                 this.workerInfo.id(), superstep);
    }

    @Override
    public void waitWorkersPrepareSuperstepDone(int superstep) {
        String path = constructPath(
                      Constants.BSP_WORKER_PREPARE_SUPERSTEP_DONE_PATH,
                      superstep);
        this.barrierOnWorkers(path, this.barrierOnWorkersTimeout);
        LOG.info("Workers prepared superstep {} done", superstep);
    }

    @Override
    public void saveDone() {
        String path = constructPath(Constants.BSP_WORKER_SAVE_DONE_PATH,
                                    this.workerInfo.id());
        this.etcdClient.put(path, Constants.EMPTY_BYTES);
        LOG.info("Worker {} save done", this.workerInfo.id());
    }

    @Override
    public void close() {
        this.etcdClient.close();
        LOG.info("Worker {} closed etcd client", this.workerInfo.id());
    }
}
