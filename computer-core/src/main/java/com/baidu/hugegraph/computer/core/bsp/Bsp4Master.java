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

import java.util.List;

import com.baidu.hugegraph.computer.core.common.ContainerInfo;
import com.baidu.hugegraph.computer.core.graph.GraphStat;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;

public interface Bsp4Master {

    // Do initialization operation, like connect to etcd cluster.
    public void init();

    // Contrary to init. Could not do any bsp operation after close is called.
    public void close();

    public void registerMaster(ContainerInfo masterInfo);

    // Wait workers registered.
    public List<ContainerInfo> waitWorkersRegistered();

    // The master determines which superstep to start from
    public void masterSuperstepResume(int superstep);

    /**
     * Wait all workers read input splits, and send all vertices and
     * edges to correspond workers. After this, master call masterInputDone.
     */
    public void waitWorkersInputDone();

    /**
     * @see Bsp4Master#waitWorkersInputDone
     */
    public void masterInputDone();

    // Wait workers finish specified super step.
    public List<WorkerStat> waitWorkersSuperstepDone(int superstep);

    /**
     * After all workers prepared superstep, master prepare superstep, and
     * call masterPrepareSuperstepDone to let the workers know that master is
     * prepared done.
     */
    public void waitWorkersSuperstepPrepared(int superstep);

    /**
     * Master signals the workers that the master superstep prepared.
     */
    public void masterSuperstepPrepared(int superstep);

    /**
     * Master signals the workers that superstep done. The workers read
     * GraphStat and determines whether to continue iteration.
     */

    public void masterSuperstepDone(int superstep, GraphStat graphStat);

    /**
     * Wait workers output the vertices.
     */
    public void waitWorkersOutputDone();

    /**
     * It's master's responsibility to clean the bsp data.
     */
    public void clean();
}
