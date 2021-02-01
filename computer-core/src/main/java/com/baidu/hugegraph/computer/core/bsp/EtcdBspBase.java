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

public abstract class EtcdBspBase {

    private static final Logger LOG = Log.logger(EtcdBspBase.class);

    private HugeConfig config;
    private EtcdClient etcdClient;
    private int workerCount;
    private long registerTimeout;
    private long barrierOnMasterTimeout;
    private long barrierOnWorkersTimeout;
    private long logInterval;

    public EtcdBspBase(HugeConfig config) {
        this.config = config;
    }

    public void init() {
        String endpoints = this.config.get(ComputerOptions.BSP_ETCD_ENDPOINTS);
        String job_id = config.get(ComputerOptions.JOB_ID);
        this.etcdClient = new EtcdClient(endpoints, job_id);
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
     * Close the connection to etcd.
     */
    public void close() {
        this.etcdClient.close();
    }

    public EtcdClient etcdClient() {
        return this.etcdClient;
    }

    public int workerCount() {
        return this.workerCount;
    }

    public long registerTimeout() {
        return this.registerTimeout;
    }

    public long barrierOnMasterTimeout() {
        return this.barrierOnMasterTimeout;
    }

    public long barrierOnWorkersTimeout() {
        return this.barrierOnWorkersTimeout;
    }

    public long logInterval() {
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
