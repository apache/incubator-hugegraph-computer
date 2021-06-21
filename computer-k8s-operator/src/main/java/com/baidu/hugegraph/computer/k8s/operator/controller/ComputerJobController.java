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

import java.util.List;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.driver.JobStatus;
import com.baidu.hugegraph.computer.k8s.Constants;
import com.baidu.hugegraph.computer.k8s.crd.model.ComponentState;
import com.baidu.hugegraph.computer.k8s.crd.model.ComponentStateBuilder;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobStatus;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobStatusBuilder;
import com.baidu.hugegraph.computer.k8s.crd.model.ConditionStatus;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJobList;
import com.baidu.hugegraph.computer.k8s.crd.model.JobComponentState;
import com.baidu.hugegraph.computer.k8s.crd.model.PodPhase;
import com.baidu.hugegraph.computer.k8s.operator.common.AbstractController;
import com.baidu.hugegraph.computer.k8s.operator.common.Request;
import com.baidu.hugegraph.computer.k8s.operator.common.Result;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
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
            return Result.REQUEUE;
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

        return Objects.equals(newStatus.getJobStatus(),
                              JobStatus.INITIALIZING.name());
    }

    private ComputerJobStatus deriveCRStatus(ComputerJobComponent observed) {
        HugeGraphComputerJob computerJob = observed.computerJob();

        MutableInt failedComponents = new MutableInt(0);
        MutableInt succeededComponents = new MutableInt(0);
        MutableInt runningComponents = new MutableInt(0);
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
        Job masterJob = observed.masterJob();
        ComponentState masterJobState = this.deriveJobStatus(
                masterJob,
                observed.masterPods(),
                status.getComponentStates().getMasterJob(),
                failedComponents,
                succeededComponents,
                runningComponents
        );
        status.getComponentStates().setMasterJob(masterJobState);

        // WorkerJob
        Job workerJob = observed.masterJob();
        ComponentState workerJobState = this.deriveJobStatus(
                workerJob,
                observed.workerPods(),
                status.getComponentStates().getWorkerJob(),
                failedComponents,
                succeededComponents,
                runningComponents
        );
        status.getComponentStates().setWorkerJob(workerJobState);

        if (failedComponents.intValue() > Constants.ALLOW_FAILED_COMPONENTS) {
            status.setJobStatus(JobStatus.FAILED.name());
            return status;
        } else if (succeededComponents.intValue() == totalComponents) {
            status.setJobStatus(JobStatus.SUCCEEDED.name());
            return status;
        }

        if (runningComponents.intValue() < totalComponents) {
            status.setJobStatus(JobStatus.INITIALIZING.name());
        } else {
            status.setJobStatus(JobStatus.RUNNING.name());
        }

        return status;
    }

    private ComponentState deriveJobStatus(Job job, List<Pod> pods,
                                           ComponentState state,
                                           MutableInt failedComponents,
                                           MutableInt succeededComponents,
                                           MutableInt runningComponents) {

        if (job != null) {
            ComponentState newState = new ComponentState();
            newState.setName(job.getMetadata().getName());

            int succeeded = KubeUtil.intVal(job.getStatus().getActive());
            int failed = KubeUtil.intVal(job.getStatus().getActive());
            Pair<Boolean, String> unSchedulablePair = this.unSchedulable(pods);

            if (succeeded >= Constants.MASTER_INSTANCES) {
                newState.setState(JobComponentState.SUCCEEDED.name());
                succeededComponents.increment();
            } else if (failed > Constants.ALLOW_FAILED_JOBS) {
                newState.setState(JobComponentState.FAILED.name());
                List<JobCondition> conditions = job.getStatus().getConditions();
                if (CollectionUtils.isNotEmpty(conditions)) {
                    newState.setMessage(conditions.get(0).getMessage());
                }
                failedComponents.increment();
            } else if (unSchedulablePair.getKey()) {
                newState.setState(JobStatus.FAILED.name());
                newState.setMessage(unSchedulablePair.getValue());
                failedComponents.increment();
            } else {
                int active = KubeUtil.intVal(job.getStatus().getActive());
                boolean allPodRunning = pods.stream().allMatch(pod -> {
                    return Objects.equals(pod.getStatus().getPhase(),
                                          PodPhase.RUNNING.value());
                });
                if (active > Constants.MASTER_INSTANCES && allPodRunning) {
                    newState.setState(JobComponentState.RUNNING.value());
                    runningComponents.increment();
                } else {
                    newState.setState(JobComponentState.PENDING.value());
                }
            }
        } else if (state != null) {
            state.setState(JobComponentState.CANCELLED.value());
        }

        return state;
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

        if (master != null) {
            List<Pod> masterPods = this.getPodsByLabels(master);
            observed.masterPods(masterPods);
        }

        String workerName = KubeUtil.workerJobName(name);
        Job worker = this.getResourceByName(namespace, workerName, Job.class);
        observed.workerJob(worker);

        if (worker != null) {
            List<Pod> workerPods = this.getPodsByLabels(worker);
            observed.workerPods(workerPods);
        }

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

    private Pair<Boolean, String> unSchedulable(List<Pod> pods) {
        if (CollectionUtils.isEmpty(pods)) {
            return Pair.of(false, null);
        }

        for (Pod pod : pods) {
            List<PodCondition> conditions = pod.getStatus()
                                               .getConditions();
            for (PodCondition condition : conditions) {
                if (Objects.equals(condition.getStatus(),
                                   ConditionStatus.FALSE.value()) &&
                    Objects.equals(condition.getReason(),
                                   Constants.POD_REASON_UNSCHEDULABLE)) {
                    return Pair.of(true, condition.getMessage());
                }
            }
        }
        return Pair.of(false, null);
    }
}
