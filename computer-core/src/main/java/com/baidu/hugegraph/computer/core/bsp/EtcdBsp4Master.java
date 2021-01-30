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
        String path = constructPath(BspEvent.BSP_MASTER_REGISTERED);
        this.etcdClient.put(path, ReadWriteUtil.toBytes(masterInfo));
        LOG.info("Master registered, masterInfo: {}", masterInfo);
    }

    public List<ContainerInfo> waitWorkersRegistered() {
        LOG.info("Master is waiting workers registered");
        String path = constructPath(BspEvent.BSP_WORKER_REGISTERED);
        List<byte[]> serializedContainers = this.waitOnWorkers(
                                            path, this.registerTimeout);
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
    public void masterSuperstepResume(int superstep) {
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_RESUME);
        IntValue superstepWritable = new IntValue(superstep);
        this.etcdClient.put(path, ReadWriteUtil.toBytes(superstepWritable));
        LOG.info("Master resume superstep {}", superstep);
    }

    @Override
    public void waitWorkersInputDone() {
        LOG.info("Master is waiting workers input done");
        String path = constructPath(BspEvent.BSP_WORKER_INPUT_DONE);
        waitOnWorkers(path, this.barrierOnWorkersTimeout);
    }

    @Override
    public void masterInputDone() {
        String path = constructPath(BspEvent.BSP_MASTER_INPUT_DONE);
        this.etcdClient.put(path, Constants.EMPTY_BYTES);
        LOG.info("Master input done");
    }

    @Override
    public List<WorkerStat> waitWorkersSuperstepDone(int superstep) {
        LOG.info("Master is waiting workers superstep {} done", superstep);
        String path = constructPath(BspEvent.BSP_WORKER_SUPERSTEP_DONE,
                                    superstep);
        List<byte[]> list = waitOnWorkers(path, this.barrierOnWorkersTimeout);
        List<WorkerStat> result = new ArrayList<>(this.workerCount);
        for (byte[] bytes : list) {
            WorkerStat workerStat = new WorkerStat();
            ReadWriteUtil.fromBytes(bytes, workerStat);
            result.add(workerStat);
        }
        LOG.info("Workers superstep {} done, workers stat: {}",
                 superstep, result);
        return result;
    }

    @Override
    public void waitWorkersSuperstepPrepared(int superstep) {
        LOG.info("Master is waiting workers prepare superstep {} done",
                 superstep);
        String path = constructPath(BspEvent.BSP_WORKER_SUPERSTEP_PREPARED,
                                    superstep);
        waitOnWorkers(path, this.barrierOnWorkersTimeout);
    }

    @Override
    public void masterSuperstepPrepared(int superstep) {
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_PREPARED,
                                    superstep);
        this.etcdClient.put(path, Constants.EMPTY_BYTES);
        LOG.info("Master prepare superstep {} done", superstep);
    }

    @Override
    public void masterSuperstepDone(int superstep, GraphStat graphStat) {
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_DONE,
                                    superstep);
        this.etcdClient.put(path, ReadWriteUtil.toBytes(graphStat));
        LOG.info("Master superstep {} done, graph stat: {}",
                 superstep, graphStat);
    }

    @Override
    public void waitWorkersOutputDone() {
        LOG.info("Master is waiting workers output done");
        String path = constructPath(BspEvent.BSP_WORKER_OUTPUT_DONE);
        waitOnWorkers(path, this.barrierOnWorkersTimeout);
    }

    @Override
    public void clean() {
        this.etcdClient.deleteAllKvsInNamespace();
        LOG.info("Clean bsp data done");
    }

    private List<byte[]> waitOnWorkers(String prefix, long timeout) {
        return this.etcdClient.getWithPrefix(prefix, this.workerCount,
                                             timeout, this.logInterval);
    }
}
