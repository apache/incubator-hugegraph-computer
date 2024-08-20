/*
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

package org.apache.hugegraph.computer.core.bsp;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public abstract class BspBase {

    private static final Logger LOG = Log.logger(BspBase.class);

    private final Config config;

    private final String jobId;
    private final String jobNamespace;
    private final int workerCount;
    private final long registerTimeout;
    private final long barrierOnMasterTimeout;
    private final long barrierOnWorkersTimeout;
    private final long logInterval;

    private final BspClient bspClient;

    public BspBase(Config config) {
        this.config = config;

        this.jobId = config.get(ComputerOptions.JOB_ID);
        this.jobNamespace = config.get(ComputerOptions.JOB_NAMESPACE);
        this.workerCount = this.config.get(ComputerOptions.JOB_WORKERS_COUNT);
        this.registerTimeout = this.config.get(
             ComputerOptions.BSP_REGISTER_TIMEOUT);
        this.barrierOnWorkersTimeout = this.config.get(
             ComputerOptions.BSP_WAIT_WORKERS_TIMEOUT);
        this.barrierOnMasterTimeout = this.config.get(
             ComputerOptions.BSP_WAIT_MASTER_TIMEOUT);
        this.logInterval = this.config.get(ComputerOptions.BSP_LOG_INTERVAL);

        this.bspClient = this.init();
    }

    /**
     * Do initialization operation, like connect to etcd or ZooKeeper cluster.
     */
    private BspClient init() {
        BspClient bspClient = this.createBspClient();
        String namespace = StringUtils.isEmpty(this.jobNamespace) ?
                           this.constructPath(null, this.jobId) :
                           this.constructPath(null, this.jobNamespace, this.jobId);
        bspClient.init(namespace);
        LOG.info("Init {} BSP connection to '{}' for job '{}'",
                 bspClient.type(), bspClient.endpoint(), this.jobId);
        return bspClient;
    }

    /**
     * Close the connection to etcd or Zookeeper. Contrary to init.
     * Could not do any bsp operation after close is called.
     */
    public void close() {
        this.bspClient.close();
        LOG.info("Closed {} BSP connection '{}' for job '{}'",
                 this.bspClient.type(), this.bspClient.endpoint(), this.jobId);
    }

    /**
     * Cleaned up the BSP data
     */
    public void clean() {
        try {
            this.bspClient().clean();
        } catch (Exception e) {
            throw new ComputerException("Failed to clean up the BSP data: %s",
                                        e, this.bspClient().endpoint());
        }
        LOG.info("Cleaned up the BSP data: {}", this.bspClient().endpoint());
    }

    private BspClient createBspClient() {
        // TODO: create from factory. the type of bsp can be get from config
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
        if (event != null) {
            // TODO: replace event.code() with event.name()
            sb.append(event.name());
        }
        for (Object path : paths) {
            sb.append("/").append(path.toString());
        }
        return sb.toString();
    }
}
