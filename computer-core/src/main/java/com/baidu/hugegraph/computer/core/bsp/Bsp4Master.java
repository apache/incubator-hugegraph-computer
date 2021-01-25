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
    void init();

    public void registerMaster(ContainerInfo masterInfo);

    // Wait workers registered.
    public List<ContainerInfo> waitWorkersRegistered();

    // The first superStep to execute.
    public void firstSuperStep(int superStep);

    // Wait workers finish specified super step.
    public List<WorkerStat> waitWorkerSuperStepDone(int superStepId);

    // Master set super step done.
    public void masterSuperStepDone(int superStep, GraphStat graphStat);

    // Wait workers have saved the vertices.
    public void waitWorkersSaveDone();

    // It's master's responsibility to clean the bsp data.
    public void cleanBspData();

    // Could not do any bsp operation after close is called.
    void close();
}
