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

package com.baidu.hugegraph.computer.k8s.operator.controller;

import java.util.Objects;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.driver.JobStatus;
import com.baidu.hugegraph.computer.k8s.Constants;
import com.baidu.hugegraph.computer.k8s.crd.model.ComponentState;
import com.baidu.hugegraph.computer.k8s.crd.model.ComponentStateBuilder;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobStatus;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobStatusBuilder;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJobList;
import com.baidu.hugegraph.computer.k8s.operator.common.AbstractController;
import com.baidu.hugegraph.computer.k8s.operator.common.Request;
import com.baidu.hugegraph.computer.k8s.operator.common.Result;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.utils.Serialization;

public class ComputerJobController
       extends AbstractController<HugeGraphComputerJob> {

    private static final Logger LOG = Log.logger(AbstractController.class);
    private static final String FINALIZER_NAME = CustomResource.getCRDName(
                         HugeGraphComputerJob.class) + "/finalizers";

    private final MixedOperation<HugeGraphComputerJob, HugeGraphComputerJobList,
            Resource<HugeGraphComputerJob>> operation;

    public ComputerJobController(HugeConfig config,
                                 KubernetesClient kubeClient) {
        super(config, kubeClient);
        this.operation = this.kubeClient.customResources(
                                         HugeGraphComputerJob.class,
                                         HugeGraphComputerJobList.class);
    }

    @Override
    protected Result reconcile(Request request) {
        HugeGraphComputerJob computerJob = this.getCR(request);
        if (computerJob == null) {
            LOG.info("Unable to fetch HugeGraphComputerJob, " +
                     "it may have been deleted");
            return Result.NO_REQUEUE;
        }

        this.fillCRStatus(computerJob);

        if (this.finalizer(computerJob)) {
            return Result.NO_REQUEUE;
        }

        ComputerJobComponent observed = this.observeComponent(computerJob);
        if (this.updateStatus(observed)) {
            LOG.info("Wait status to be stable before taking further actions");
            return Result.NO_REQUEUE;
        }

        // TODO: implement it
        return Result.NO_REQUEUE;
    }

    private boolean updateStatus(ComputerJobComponent observed) {
        ComputerJobStatus newStatus = this.deriveCRStatus(observed);
        ComputerJobStatus oldStatus = observed.computerJob().getStatus();
        if (!Objects.deepEquals(oldStatus, newStatus)) {
            HugeGraphComputerJob computerJob = observed.computerJob();
            computerJob.setStatus(newStatus);
            this.updateStatus(computerJob);
            return true;
        }
        return false;
    }

    private ComputerJobStatus deriveCRStatus(ComputerJobComponent observed) {
        HugeGraphComputerJob computerJob = observed.computerJob();

        boolean jobFailed = false;
        boolean jobSucceeded = false;

        int runningComponents = 0;
        int totalComponents = Constants.TOTAL_COMPONENTS;

        ComputerJobStatus status = Serialization.clone(computerJob.getStatus());

        // ConfigMap
        ConfigMap configMap = observed.configMap();
        if (configMap != null) {
            ComponentState configMapState = new ComponentStateBuilder()
                    .withName(configMap.getMetadata().getName())
                    .withState(Constants.COMPONENT_STATE_READY)
                    .build();
            status.getComponentStates().setConfigMap(configMapState);
        } else if (status.getComponentStates().getConfigMap() != null) {
            status.getComponentStates().getConfigMap()
                  .setState(Constants.COMPONENT_STATE_DELETED);
        }

        // MasterJob
        Job master = observed.masterJob();
        String masterFailedMessage;
        if (master != null) {
            ComponentState masterState = new ComponentState();
            masterState.setName(master.getMetadata().getName());

            int active = KubeUtil.intVal(master.getStatus().getActive());
            if (active > Constants.MASTER_INSTANCES) {
                runningComponents++;
                masterState.setState(Constants.COMPONENT_STATE_READY);
            } else {
                masterState.setState(Constants.COMPONENT_STATE_NOT_READY);
            }

            int succeeded = KubeUtil.intVal(master.getStatus().getActive());
            int failed = KubeUtil.intVal(master.getStatus().getActive());
            if (succeeded >= Constants.MASTER_INSTANCES) {
                masterState.setState(JobStatus.SUCCEEDED.name());
                jobSucceeded = true;
            } else if (failed > Constants.ALLOW_FAILED_JOB) {
                masterState.setState(JobStatus.FAILED.name());
                jobFailed = true;
            }
            status.getComponentStates().setMasterJob(masterState);
        } else if (status.getComponentStates().getMasterJob() != null) {
            status.getComponentStates().getMasterJob()
                  .setState(Constants.COMPONENT_STATE_DELETED);
        }

        // WorkerJob
        Job worker = observed.workerJob();
        if (worker != null) {
            ComponentState workerState = new ComponentState();
            workerState.setName(worker.getMetadata().getName());

            int active = KubeUtil.intVal(worker.getStatus().getActive());
            if (active >= computerJob.getSpec().getWorkerInstances()) {
                runningComponents++;
                workerState.setState(Constants.COMPONENT_STATE_READY);
            } else {
                workerState.setState(Constants.COMPONENT_STATE_NOT_READY);
            }

            int succeeded = KubeUtil.intVal(worker.getStatus().getSucceeded());
            int failed = KubeUtil.intVal(worker.getStatus().getActive());
            if (succeeded >= computerJob.getSpec().getWorkerInstances()) {
                workerState.setState(JobStatus.SUCCEEDED.name());
                jobSucceeded = true;
            } else if (failed > Constants.ALLOW_FAILED_JOB) {
                workerState.setState(JobStatus.FAILED.name());
                jobFailed = true;
            }
            status.getComponentStates().setWorkerJob(workerState);
        } else if (status.getComponentStates().getWorkerJob() != null) {
            status.getComponentStates().getWorkerJob()
                  .setState(Constants.COMPONENT_STATE_DELETED);
        }

        // Derive the status
        if (jobFailed || jobSucceeded) {
            if (jobFailed) {
                status.setJobStatus(JobStatus.FAILED.name());
            } else {
                status.setJobStatus(JobStatus.SUCCEEDED.name());
            }
            return status;
        }

        if (runningComponents < totalComponents) {
            status.setJobStatus(JobStatus.INITIALIZING.name());
        } else {
            status.setJobStatus(JobStatus.RUNNING.name());
        }

        return status;
    }

    private void fillCRStatus(HugeGraphComputerJob computerJob) {
        ComputerJobStatus status = computerJob.getStatus() == null ?
                                   new ComputerJobStatus() :
                                   computerJob.getStatus();
        status = new ComputerJobStatusBuilder(status)
                .editOrNewComponentStates().endComponentStates()
                .editOrNewJobState().endJobState()
                .build();
        computerJob.setStatus(status);
    }

    private ComputerJobComponent observeComponent(
            HugeGraphComputerJob computerJob) {
        ComputerJobComponent observed = new ComputerJobComponent();
        observed.computerJob(computerJob);

        String namespace = computerJob.getMetadata().getNamespace();
        String name = computerJob.getMetadata().getName();

        String masterName = KubeUtil.masterJobName(name);
        Job master = this.getResourceByName(namespace, masterName, Job.class);
        observed.masterJob(master);

        String workerName = KubeUtil.workerJobName(name);
        Job worker = this.getResourceByName(namespace, workerName, Job.class);
        observed.workerJob(worker);

        String configMapName = KubeUtil.configMapName(name);
        ConfigMap configMap = this.getResourceByName(namespace, configMapName,
                                                     ConfigMap.class);
        observed.configMap(configMap);

        return observed;
    }

    private boolean finalizer(HugeGraphComputerJob computerJob) {
        if (computerJob.addFinalizer(FINALIZER_NAME)) {
            this.replaceCR(computerJob);
            return true;
        }

        ComputerJobStatus status = computerJob.getStatus();
        if (computerJob.isMarkedForDeletion()) {
            if (!JobStatus.finished(status.getJobStatus())) {
                status.setJobStatus(JobStatus.CANCELLED.name());
                this.updateStatus(computerJob);
                return true;
            } else {
                if (computerJob.removeFinalizer(FINALIZER_NAME)) {
                    this.replaceCR(computerJob);
                    return true;
                }
            }
        } else {
            if (JobStatus.finished(status.getJobStatus())) {
                this.deleteCR(computerJob);
                return true;
            }
        }

        return false;
    }

    private void updateStatus(HugeGraphComputerJob computerJob) {
        computerJob.getStatus().setLastUpdateTime(KubeUtil.now());
        this.operation.inNamespace(computerJob.getMetadata().getNamespace())
                      .updateStatus(computerJob);
    }

    private void replaceCR(HugeGraphComputerJob computerJob) {
        computerJob.getStatus().setLastUpdateTime(KubeUtil.now());
        this.operation.inNamespace(computerJob.getMetadata().getNamespace())
                      .replace(computerJob);
    }

    private void deleteCR(HugeGraphComputerJob computerJob) {
        this.operation.inNamespace(computerJob.getMetadata().getNamespace())
                      .delete(computerJob);
    }
}
