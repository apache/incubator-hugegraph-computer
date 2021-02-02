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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public abstract class BspBase {

    private static final Logger LOG = Log.logger(BspBase.class);

    private HugeConfig config;
    private BspClient bspClient;
    private int workerCount;
    private long registerTimeout;
    private long barrierOnMasterTimeout;
    private long barrierOnWorkersTimeout;
    private long logInterval;

    public BspBase(HugeConfig config) {
        this.config = config;
    }

    /**
     * Do initialization operation, like connect to etcd or ZooKeeper cluster.
     */
    public void init() {
        this.bspClient = this.createBspClient();
        this.bspClient.init();
        this.workerCount = this.config.get(ComputerOptions.JOB_WORKERS_COUNT);
        this.registerTimeout = this.config.get(
                               ComputerOptions.BSP_REGISTER_TIMEOUT);
        this.barrierOnWorkersTimeout = this.config.get(
             ComputerOptions.BSP_BARRIER_ON_WORKERS_TIMEOUT);
        this.barrierOnMasterTimeout = this.config.get(
             ComputerOptions.BSP_BARRIER_ON_MASTER_TIMEOUT);
        this.logInterval = this.config.get(ComputerOptions.BSP_LOG_INTERVAL);
    }

    /**
     * Close the connection to etcd or Zookeeper. Contrary to init.
     * Could not do any bsp operation after close is called.
     */
    public void close() {
        this.bspClient.close();
    }

    private BspClient createBspClient() {
        // TODO: the type of bsp client can be get from config
        return new EtcdBspClient(this.config);
    }

    protected final BspClient bspClient() {
        return this.bspClient;
    }

    public final int workerCount() {
        return this.workerCount;
    }

    public final long registerTimeout() {
        return this.registerTimeout;
    }

    public final long barrierOnMasterTimeout() {
        return this.barrierOnMasterTimeout;
    }

    public final long barrierOnWorkersTimeout() {
        return this.barrierOnWorkersTimeout;
    }

    public final long logInterval() {
        return this.logInterval;
    }

    /**
     * This method is used to generate the key saved in etcd. We can add
     * attempt id in the path for further checkpoint based implementation.
     */
    protected String constructPath(BspEvent event, Object... paths) {
        StringBuilder sb = new StringBuilder();
        sb.append(event.code());
        for (Object path : paths) {
            sb.append("/").append(path.toString());
        }
        return sb.toString();
    }
}
