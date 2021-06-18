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

package com.baidu.hugegraph.computer.k8s.driver;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.computer.driver.ComputerDriver;
import com.baidu.hugegraph.computer.driver.JobObserver;
import com.baidu.hugegraph.computer.driver.JobState;
import com.baidu.hugegraph.computer.driver.SuperstepStat;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJobList;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

public class KubernetesDriver implements ComputerDriver {

    private final KubernetesClient kubeClient;
    private final MixedOperation<HugeGraphComputerJob, HugeGraphComputerJobList,
            Resource<HugeGraphComputerJob>> operation;

    public KubernetesDriver() {
        this.kubeClient = new DefaultKubernetesClient();
        this.operation = this.kubeClient.customResources(
                         HugeGraphComputerJob.class,
                         HugeGraphComputerJobList.class);
    }

    @Override
    public void uploadAlgorithmJar(String algorithmName, InputStream input) {
        // TODO: implement
    }

    @Override
    public String submitJob(String algorithmName, Map<String, String> params) {
        // TODO: implement
        return null;
    }

    @Override
    public void cancelJob(String jobId, Map<String, String> params) {
        // TODO: implement
    }

    @Override
    public void waitJob(String jobId, Map<String, String> params,
                        JobObserver observer) {
        // TODO: implement
    }

    @Override
    public JobState jobState(String jobId, Map<String, String> params) {
        // TODO: implement
        return null;
    }

    @Override
    public List<SuperstepStat> superstepStats(String jobId,
                                              Map<String, String> params) {
        // TODO: implement
        return null;
    }

    @Override
    public String diagnostics(String jobId, Map<String, String> params) {
        // TODO: implement
        return null;
    }

    @Override
    public String log(String jobId, int containerId, long offset, long length,
                      Map<String, String> params) {
        // TODO: implement
        return null;
    }

    public void close() {
        this.kubeClient.close();
    }
}
