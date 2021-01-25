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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerOptions;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public abstract class EtcdBspBase {

    private static final Logger LOG = Log.logger(EtcdBspBase.class);

    protected HugeConfig config;
    protected EtcdClient etcdClient;
    protected int workerCount;
    protected long registerTimeout;
    protected long barrierOnMasterTimeout;
    protected long barrierOnWorkerTimeout;
    protected long logInterval;

    public EtcdBspBase(HugeConfig config) {
        this.config = config;
    }

    public void init() {
        String endpoints = this.config.get(ComputerOptions.ETCD_ENDPOINTS);
        String job_id = config.get(ComputerOptions.JOB_ID);
        this.etcdClient = new EtcdClient(endpoints, job_id);
        this.workerCount = this.config.get(ComputerOptions.WORKER_COUNT);
        this.registerTimeout = this.config.get(
                               ComputerOptions.BSP_REGISTER_TIMEOUT);
        this.barrierOnWorkerTimeout = this.config.get(
             ComputerOptions.BSP_BARRIER_ON_WORKERS_TIMEOUT);
        this.barrierOnMasterTimeout = this.config.get(
             ComputerOptions.BSP_BARRIER_ON_MASTER_TIMEOUT);
        this.logInterval = this.config.get(ComputerOptions.BSP_LOG_INTERVAL);
    }

    protected List<byte[]> barrierOnWorkers(String prefix, long timeout) {
        long deadLine = System.currentTimeMillis() + timeout;
        List<byte[]> result;
        while (System.currentTimeMillis() < deadLine) {
            result = this.etcdClient.getWithPrefix(prefix, this.workerCount,
                                                   this.logInterval, false);
            if (result.size() == this.workerCount) {
                return result;
            } else if (result.size() < this.workerCount) {
                LOG.info("Only {} out of {} workers finished", result.size(),
                         this.workerCount);
            } else {
                assert false;
                throw new ComputerException("Expected %s result, but got %s",
                                            this.workerCount, result.size());
            }
        }
        throw new ComputerException("Workers not finished in %s ms", timeout);
    }

    /**
     * Close the connection to etcd.
     */
    public void close() {
        this.etcdClient.close();
    }

    /**
     * This method is used to generate the key saved in etcd. We can add
     * attempt id in the path for further checkpoint based implementation.
     */
    protected String constructPath(String parent, Object... paths) {
        E.checkArgumentNotNull(parent, "The parent can't be null");
        StringBuilder sb = new StringBuilder(parent);
        for (Object path : paths) {
            sb.append("/").append(path.toString());
        }
        return sb.toString();
    }
}
