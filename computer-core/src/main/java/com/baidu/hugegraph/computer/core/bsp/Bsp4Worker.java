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

public interface Bsp4Worker {

    // Do initialization operation, like connect to etcd cluster.
    public void init();

    /**
     * Register this worker, worker's information is passed by constructor of
     * subclass.
     */
    public void registerWorker();

    /**
     * Wait master registered, get master's information includes hostname
     * and port.
     */
    public ContainerInfo waitMasterRegistered();

    /**
     * Get all workers information includes hostname and port the workers
     * listen on.
     */
    public List<ContainerInfo> waitWorkersRegistered();

    /**
     * The master set this signal to let workers knows the first superstep to
     * start with.
     */
    public int waitFirstSuperstep();

    /**
     * Set read done signal after read input splits, and send all vertices and
     * edges to correspond workers.
     */
    public void readDone();

    /**
     * Wait all workers read vertex and edge from input. After this, worker
     * can merge the vertices and edges.
     */
    public void waitWorkersReadDone();

    /**
     * Worker set this signal after sent all messages to corresponding
     * workers and sent aggregators to master.
     */
    public void superstepDone(int superstep, WorkerStat workerStat);

    /**
     * The master set this signal after all workers signaled superstepDone,
     * and master computes MasterComputation, and broadcast all aggregates to
     * works.
     */
    public GraphStat waitMasterSuperstepDone(int superstep);

    /**
     * Worker set this signal to indicate the worker is ready to receive
     * messages from other workers.
     */
    public void prepareSuperstepDone(int superstep);

    /**
     * After all workers prepared, the worker can execute and send messages
     * to other workers.
     */
    public void waitWorkersPrepareSuperstepDone(int superstep);

    /**
     * Worker set this signal to indicate the worker has saved the result.
     * It can successfully exit.
     */
    public void saveDone();

    // Contrary to init. Could not do any bsp operation after close is called.
    public void close();
}
