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

import com.baidu.hugegraph.computer.core.common.ComputerOptions;
import com.baidu.hugegraph.config.HugeConfig;

public final class EtcdBspClient implements BspClient {

    private HugeConfig config;
    private EtcdClient etcdClient;

    public EtcdBspClient(HugeConfig config) {
        this.config = config;
    }

    @Override
    public void init() {
        String endpoints = this.config.get(ComputerOptions.BSP_ETCD_ENDPOINTS);
        String jobId = this.config.get(ComputerOptions.JOB_ID);
        this.etcdClient = new EtcdClient(endpoints, jobId);
    }

    @Override
    public void close() {
        this.etcdClient.close();
    }

    @Override
    public void clean() {
        this.etcdClient.deleteAllKvsInNamespace();
    }

    @Override
    public void put(String key, byte[] value) {
        this.etcdClient.put(key, value);
    }

    @Override
    public byte[] get(String key) {
        return this.etcdClient.get(key);
    }

    @Override
    public byte[] get(String key, long timeout) {
        return this.etcdClient.get(key, timeout);
    }

    @Override
    public List<byte[]> getChildren(String prefix, int expectedCount,
                                    long timeout, long logInterval) {
        return this.etcdClient.getWithPrefix(prefix, expectedCount,
                                             timeout, logInterval);
    }
}
