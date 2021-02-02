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

public class Bsp4Master extends BspBase {

    private static final Logger LOG = Log.logger(Bsp4Master.class);

    public Bsp4Master(HugeConfig config) {
        super(config);
    }

    /**
     * Register Master, workers can get master information.
     */
    public void registerMaster(ContainerInfo masterInfo) {
        String path = constructPath(BspEvent.BSP_MASTER_REGISTERED);
        bspClient().put(path, ReadWriteUtil.toBytes(masterInfo));
        LOG.info("Master registered, masterInfo: {}", masterInfo);
    }

    /**
     * Wait workers registered.
     */
    public List<ContainerInfo> waitWorkersRegistered() {
        LOG.info("Master is waiting workers registered");
        String path = constructPath(BspEvent.BSP_WORKER_REGISTERED);
        List<byte[]> serializedContainers = this.waitOnWorkersEvent(
                                            path, registerTimeout());
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
     * The master determines which superstep to start from
     */
    public void masterSuperstepResume(int superstep) {
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_RESUME);
        IntValue superstepWritable = new IntValue(superstep);
        bspClient().put(path, ReadWriteUtil.toBytes(superstepWritable));
        LOG.info("Master resume superstep {}", superstep);
    }

    /**
     * Wait all workers read input splits, and send all vertices and
     * edges to correspond workers. After this, master call masterInputDone.
     */
    public void waitWorkersInputDone() {
        LOG.info("Master is waiting workers input done");
        String path = constructPath(BspEvent.BSP_WORKER_INPUT_DONE);
        waitOnWorkersEvent(path, barrierOnWorkersTimeout());
    }

    /**
     * The master signal workers the master input done, the workers can merge
     * vertices and edges after receive this signal.
     */
    public void masterInputDone() {
        String path = constructPath(BspEvent.BSP_MASTER_INPUT_DONE);
        bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Master input done");
    }

    /**
     * Wait workers finish specified superstep. The master receives the
     * worker stat from all workers, calls algorithm's master computation,
     * check the max iteration count, and then calls masterSuperstepDone to
     * synchronize superstep result.
     */
    public List<WorkerStat> waitWorkersSuperstepDone(int superstep) {
        LOG.info("Master is waiting workers superstep {} done", superstep);
        String path = constructPath(BspEvent.BSP_WORKER_SUPERSTEP_DONE,
                                    superstep);
        List<byte[]> list = waitOnWorkersEvent(path, barrierOnWorkersTimeout());
        List<WorkerStat> result = new ArrayList<>(workerCount());
        for (byte[] bytes : list) {
            WorkerStat workerStat = new WorkerStat();
            ReadWriteUtil.fromBytes(bytes, workerStat);
            result.add(workerStat);
        }
        LOG.info("Workers superstep {} done, workers stat: {}",
                 superstep, result);
        return result;
    }

    /**
     * After all workers prepared superstep, master prepare superstep, and
     * call masterPrepareSuperstepDone to let the workers know that master is
     * prepared done.
     */
    public void waitWorkersSuperstepPrepared(int superstep) {
        LOG.info("Master is waiting workers prepare superstep {} done",
                 superstep);
        String path = constructPath(BspEvent.BSP_WORKER_SUPERSTEP_PREPARED,
                                    superstep);
        waitOnWorkersEvent(path, barrierOnWorkersTimeout());
    }

    /**
     * Master signals the workers that the master superstep prepared.
     */
    public void masterSuperstepPrepared(int superstep) {
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_PREPARED,
                                    superstep);
        bspClient().put(path, Constants.EMPTY_BYTES);
        LOG.info("Master prepare superstep {} done", superstep);
    }

    /**
     * Master signals the workers that superstep done. The workers read
     * GraphStat and determines whether to continue iteration.
     */
    public void masterSuperstepDone(int superstep, GraphStat graphStat) {
        String path = constructPath(BspEvent.BSP_MASTER_SUPERSTEP_DONE,
                                    superstep);
        bspClient().put(path, ReadWriteUtil.toBytes(graphStat));
        LOG.info("Master superstep {} done, graph stat: {}",
                 superstep, graphStat);
    }

    /**
     * Wait workers output the vertices.
     */
    public void waitWorkersOutputDone() {
        LOG.info("Master is waiting workers output done");
        String path = constructPath(BspEvent.BSP_WORKER_OUTPUT_DONE);
        waitOnWorkersEvent(path, barrierOnWorkersTimeout());
    }

    public void clean() {
        bspClient().clean();
        LOG.info("Clean bsp data done");
    }

    private List<byte[]> waitOnWorkersEvent(String prefix, long timeout) {
        return bspClient().getChildren(prefix, workerCount(), timeout,
                                       logInterval());
    }
}
