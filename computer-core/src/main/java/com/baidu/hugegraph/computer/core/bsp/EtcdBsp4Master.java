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

public class EtcdBsp4Master extends EtcdBspBase implements Bsp4Master {

    private static final Logger LOG = Log.logger(EtcdBsp4Master.class);

    public EtcdBsp4Master(HugeConfig config) {
        super(config);
    }

    public void registerMaster(ContainerInfo masterInfo) {
        String path = constructPath(Constants.BSP_MASTER_REGISTER_PATH);
        this.etcdClient.put(path, ReadWriteUtil.toByteArray(masterInfo));
        LOG.info("Master registered, masterInfo:{}", masterInfo);
    }

    public List<ContainerInfo> waitWorkersRegistered() {
        String path = constructPath(Constants.BSP_WORKER_REGISTER_PATH);
        List<byte[]> serializedContainers = this.barrierOnWorkers(path,
                                                 this.registerTimeout);

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
    public void firstSuperstep(int superstep) {
        String path = constructPath(Constants.BSP_MASTER_FIRST_SUPER_STEP_PATH);
        IntValue superstepWritable = new IntValue(superstep);
        this.etcdClient.put(path, ReadWriteUtil.toByteArray(superstepWritable));
        LOG.info("First super step {}", superstep);
    }

    @Override
    public List<WorkerStat> waitWorkersSuperstepDone(int superstep) {
        String path = constructPath(Constants.BSP_WORKER_SUPER_STEP_DONE_PATH,
                                    superstep);
        List<byte[]> list = barrierOnWorkers(path,
                                             this.barrierOnWorkersTimeout);
        List<WorkerStat> result = new ArrayList<>(this.workerCount);
        for (byte[] bytes : list) {
            WorkerStat workerStat = new WorkerStat();
            ReadWriteUtil.readFrom(bytes, workerStat);
            result.add(workerStat);
        }
        LOG.info("Workers super step {} done, workers stat:{}", superstep,
                 result);
        return result;
    }

    @Override
    public void masterSuperstepDone(int superstep, GraphStat graphStat) {
        String path = constructPath(Constants.BSP_MASTER_SUPER_STEP_DONE_PATH,
                                    superstep);
        this.etcdClient.put(path, ReadWriteUtil.toByteArray(graphStat));
        LOG.info("Master super step {} done, graph stat:{}",
                 superstep, graphStat);
    }

    @Override
    public void waitWorkersSaveDone() {
        String path = constructPath(Constants.BSP_WORKER_SAVE_DONE_PATH);
        barrierOnWorkers(path, this.barrierOnWorkersTimeout);
        LOG.info("Workers save done");
    }

    @Override
    public void cleanBspData() {
        this.etcdClient.deleteAllKvsInNamespace();
        LOG.info("Clean bsp data done");
    }
}
