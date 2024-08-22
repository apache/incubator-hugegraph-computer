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

package org.apache.hugegraph.computer.k8s.operator.controller;

import java.util.List;

import org.apache.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;

public class ComputerJobComponent {

    private HugeGraphComputerJob computerJob;
    private Job masterJob;
    private Job workerJob;
    private List<Pod> masterPods;
    private List<Pod> workerPods;
    private ConfigMap configMap;

    public HugeGraphComputerJob computerJob() {
        return this.computerJob;
    }

    public void computerJob(HugeGraphComputerJob computerJob) {
        this.computerJob = computerJob;
    }

    public List<Pod> masterPods() {
        return this.masterPods;
    }

    public void masterPods(List<Pod> masterPods) {
        this.masterPods = masterPods;
    }

    public List<Pod> workerPods() {
        return this.workerPods;
    }

    public void workerPods(List<Pod> workerPods) {
        this.workerPods = workerPods;
    }

    public Job masterJob() {
        return this.masterJob;
    }

    public void masterJob(Job masterJob) {
        this.masterJob = masterJob;
    }

    public Job workerJob() {
        return this.workerJob;
    }

    public void workerJob(Job workerJob) {
        this.workerJob = workerJob;
    }

    public ConfigMap configMap() {
        return this.configMap;
    }

    public void configMap(ConfigMap configMap) {
        this.configMap = configMap;
    }
}
