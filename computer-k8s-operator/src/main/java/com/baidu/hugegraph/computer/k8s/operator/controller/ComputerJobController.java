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
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.driver.JobStatus;
import com.baidu.hugegraph.computer.k8s.Constants;
import com.baidu.hugegraph.computer.k8s.crd.model.CommonComponentState;
import com.baidu.hugegraph.computer.k8s.crd.model.ComponentState;
import com.baidu.hugegraph.computer.k8s.crd.model.ComponentStateBuilder;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobSpec;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobStatus;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobStatusBuilder;
import com.baidu.hugegraph.computer.k8s.crd.model.EventType;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJobList;
import com.baidu.hugegraph.computer.k8s.crd.model.JobComponentState;
import com.baidu.hugegraph.computer.k8s.crd.model.PodPhase;
import com.baidu.hugegraph.computer.k8s.operator.common.AbstractController;
import com.baidu.hugegraph.computer.k8s.operator.common.MatchWithMsg;
import com.baidu.hugegraph.computer.k8s.operator.common.OperatorRequest;
import com.baidu.hugegraph.computer.k8s.operator.common.OperatorResult;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.utils.PodStatusUtil;
import io.fabric8.kubernetes.client.utils.Serialization;

public class ComputerJobController
       extends AbstractController<HugeGraphComputerJob> {

    private static final Logger LOG = Log.logger(AbstractController.class);

    private final MixedOperation<HugeGraphComputerJob, HugeGraphComputerJobList,
            Resource<HugeGraphComputerJob>> operation;

    private static final int TOTAL_COMPONENTS = 2;
    private static final int ALLOW_FAILED_JOBS = 0;
    private static final int ALLOW_FAILED_COMPONENTS = 0;
    private static final int ERROR_LOG_TAILING_LINES = 500;
    private static final String POD_REASON_UNSCHEDULABLE = "Unschedulable";
    private static final String IMAGE_PULL_BACKOFF = "ImagePullBackOff";
    private static final String CONDITION_STATUS_FALSE = "False";
    private static final String FINALIZER_NAME = CustomResource.getCRDName(
            HugeGraphComputerJob.class) + "/finalizers";

    public ComputerJobController(HugeConfig config,
                                 NamespacedKubernetesClient kubeClient) {
        super(config, kubeClient);
        this.operation = this.kubeClient.customResources(
                                         HugeGraphComputerJob.class,
                                         HugeGraphComputerJobList.class);
    }

    @Override
    protected OperatorResult reconcile(OperatorRequest request) {
        HugeGraphComputerJob computerJob = this.getCR(request);
        if (computerJob == null) {
            LOG.info("Unable to fetch HugeGraphComputerJob, " +
                     "it may have been deleted");
            return OperatorResult.NO_REQUEUE;
        }

        this.fillCRStatus(computerJob);

        if (this.finalizer(computerJob)) {
            return OperatorResult.NO_REQUEUE;
        }

        ComputerJobComponent observed = this.observeComponent(computerJob);
        if (this.updateStatus(observed)) {
            LOG.info("Wait status to be stable before taking further actions");
            return OperatorResult.REQUEUE;
        }

        if (Objects.equals(computerJob.getStatus().getJobStatus(),
                           JobStatus.RUNNING.name())) {
            String crName = computerJob.getMetadata().getName();
            LOG.info("ComputerJob {} already running, no action", crName);
            return OperatorResult.NO_REQUEUE;
        }

        ComputerJobDeployer deployer = new ComputerJobDeployer(this.kubeClient,
                                                               this.config);
        deployer.deploy(observed);

        return OperatorResult.NO_REQUEUE;
    }

    @Override
    protected MatchWithMsg ownsPredicate(HasMetadata ownsResource) {
        MatchWithMsg ownsMatch = super.ownsPredicate(ownsResource);
        if (ownsMatch.isMatch()) {
            return ownsMatch;
        }

        if (ownsResource instanceof Pod) {
            ObjectMeta metadata = ownsResource.getMetadata();
            if (metadata != null) {
                Map<String, String> labels = metadata.getLabels();
                String kind = HasMetadata.getKind(HugeGraphComputerJob.class);
                String crName = KubeUtil.matchKindAndGetCrName(labels, kind);
                if (StringUtils.isNotBlank(crName)) {
                    return new MatchWithMsg(true, crName);
                }
            }
        }
        return MatchWithMsg.NO_MATCH;
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
            } else {
                if (computerJob.removeFinalizer(FINALIZER_NAME)) {
                    this.replaceCR(computerJob);
                }
            }
            return true;
        } else {
            if (JobStatus.finished(status.getJobStatus())) {
                this.deleteCR(computerJob);
                return true;
            }
        }

        return false;
    }

    private boolean updateStatus(ComputerJobComponent observed) {
        ComputerJobStatus newStatus = this.derivedCRStatus(observed);
        ComputerJobStatus oldStatus = observed.computerJob().getStatus();
        if (!Objects.deepEquals(oldStatus, newStatus)) {
            HugeGraphComputerJob computerJob = observed.computerJob();
            computerJob.setStatus(newStatus);
            this.updateStatus(computerJob);
            return true;
        }

        return false;
    }

    private ComputerJobStatus derivedCRStatus(ComputerJobComponent observed) {
        HugeGraphComputerJob computerJob = observed.computerJob();
        ComputerJobSpec spec = computerJob.getSpec();

        MutableInt failedComponents = new MutableInt(0);
        MutableInt succeededComponents = new MutableInt(0);
        MutableInt runningComponents = new MutableInt(0);

        ComputerJobStatus status = Serialization.clone(computerJob.getStatus());

        // ConfigMap
        ConfigMap configMap = observed.configMap();
        if (configMap != null) {
            ComponentState configMapState = new ComponentStateBuilder()
                    .withName(configMap.getMetadata().getName())
                    .withState(CommonComponentState.READY.value())
                    .build();
            status.getComponentStates().setConfigMap(configMapState);
        } else if (status.getComponentStates().getConfigMap() != null) {
            status.getComponentStates().getConfigMap()
                  .setState(CommonComponentState.DELETED.value());
        }

        // MasterJob
        Job masterJob = observed.masterJob();
        ComponentState masterJobState = this.deriveJobStatus(
                masterJob,
                observed.masterPods(),
                status.getComponentStates().getMasterJob(),
                Constants.MASTER_INSTANCES,
                failedComponents,
                succeededComponents,
                runningComponents
        );
        status.getComponentStates().setMasterJob(masterJobState);

        // WorkerJob
        Job workerJob = observed.workerJob();
        ComponentState workerJobState = this.deriveJobStatus(
                workerJob,
                observed.workerPods(),
                status.getComponentStates().getWorkerJob(),
                spec.getWorkerInstances(),
                failedComponents,
                succeededComponents,
                runningComponents
        );
        status.getComponentStates().setWorkerJob(workerJobState);

        if (failedComponents.intValue() > ALLOW_FAILED_COMPONENTS) {
            status.setJobStatus(JobStatus.FAILED.name());
            this.recordFailedEvent(computerJob, masterJobState, workerJobState);
            return status;
        } else if (succeededComponents.intValue() == TOTAL_COMPONENTS) {
            status.setJobStatus(JobStatus.SUCCEEDED.name());
            String crName = computerJob.getMetadata().getName();
            this.recordEvent(computerJob, EventType.NORMAL,
                             KubeUtil.succeedEventName(crName),
                             "ComputerJobSucceed",
                             String.format("Job %s run success", crName));
            return status;
        }

        int activeComponents = runningComponents.intValue() +
                               succeededComponents.intValue();
        if (activeComponents == TOTAL_COMPONENTS) {
            status.setJobStatus(JobStatus.RUNNING.name());
        } else {
            status.setJobStatus(JobStatus.INITIALIZING.name());
        }

        return status;
    }

    private ComponentState deriveJobStatus(Job job,
                                           List<Pod> pods,
                                           ComponentState oldSate,
                                           int instances,
                                           MutableInt failedComponents,
                                           MutableInt succeededComponents,
                                           MutableInt runningComponents) {
        if (job != null && job.getStatus() != null) {
            ComponentState newState = new ComponentState();
            newState.setName(job.getMetadata().getName());

            int succeeded = KubeUtil.intVal(job.getStatus().getSucceeded());
            int failed = KubeUtil.intVal(job.getStatus().getFailed());
            MatchWithMsg unSchedulable = this.unSchedulable(pods);
            MatchWithMsg failedPullImage = this.imagePullBackOff(pods);

            if (succeeded >= instances) {
                newState.setState(JobComponentState.SUCCEEDED.name());
                succeededComponents.increment();
            } else if (failed > ALLOW_FAILED_JOBS) {
                newState.setState(JobComponentState.FAILED.name());
                List<JobCondition> conditions = job.getStatus().getConditions();
                if (CollectionUtils.isNotEmpty(conditions)) {
                    newState.setMessage(conditions.get(0).getMessage());
                }
                String errorLog = this.getErrorLog(pods);
                if (StringUtils.isNotBlank(errorLog)) {
                    newState.setErrorLog(errorLog);
                }
                failedComponents.increment();
            } else if (unSchedulable.isMatch()) {
                newState.setState(JobStatus.FAILED.name());
                newState.setMessage(unSchedulable.msg());
                failedComponents.increment();
            } else if (failedPullImage.isMatch()) {
                newState.setState(JobStatus.FAILED.name());
                newState.setMessage(failedPullImage.msg());
                failedComponents.increment();
            } else {
                int active = KubeUtil.intVal(job.getStatus().getActive());
                boolean allPodRunning = pods.stream()
                                            .allMatch(PodStatusUtil::isRunning);
                if (active >= instances && allPodRunning) {
                    newState.setState(JobComponentState.RUNNING.value());
                    runningComponents.increment();
                } else {
                    newState.setState(JobComponentState.PENDING.value());
                }
            }
            return newState;
        } else if (oldSate != null) {
            oldSate.setState(JobComponentState.CANCELLED.value());
        }

        return oldSate;
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
        String crName = computerJob.getMetadata().getName();

        String masterName = KubeUtil.masterJobName(crName);
        Job master = this.getResourceByName(namespace, masterName, Job.class);
        observed.masterJob(master);

        if (master != null) {
            List<Pod> masterPods = this.getPodsByJob(master);
            observed.masterPods(masterPods);
        }

        String workerName = KubeUtil.workerJobName(crName);
        Job worker = this.getResourceByName(namespace, workerName, Job.class);
        observed.workerJob(worker);

        if (worker != null) {
            List<Pod> workerPods = this.getPodsByJob(worker);
            observed.workerPods(workerPods);
        }

        String configMapName = KubeUtil.configMapName(crName);
        ConfigMap configMap = this.getResourceByName(namespace, configMapName,
                                                     ConfigMap.class);
        observed.configMap(configMap);

        return observed;
    }

    private void updateStatus(HugeGraphComputerJob computerJob) {
        computerJob.getStatus().setLastUpdateTime(KubeUtil.now());
        String namespace = computerJob.getMetadata().getNamespace();
        if (Objects.equals(this.kubeClient.getNamespace(), namespace)) {
            this.operation.replaceStatus(computerJob);
        } else {
            this.operation.inNamespace(namespace).replaceStatus(computerJob);
        }
    }

    private void replaceCR(HugeGraphComputerJob computerJob) {
        computerJob.getStatus().setLastUpdateTime(KubeUtil.now());
        String namespace = computerJob.getMetadata().getNamespace();
        if (Objects.equals(this.kubeClient.getNamespace(), namespace)) {
            this.operation.replace(computerJob);
        } else {
            this.operation.inNamespace(namespace).replace(computerJob);
        }
    }

    private void deleteCR(HugeGraphComputerJob computerJob) {
        String namespace = computerJob.getMetadata().getNamespace();
        if (Objects.equals(this.kubeClient.getNamespace(), namespace)) {
            this.operation.delete(computerJob);
        } else {
            this.operation.inNamespace(namespace).delete(computerJob);
        }
    }

    private MatchWithMsg unSchedulable(List<Pod> pods) {
        if (CollectionUtils.isEmpty(pods)) {
            return MatchWithMsg.NO_MATCH;
        }

        for (Pod pod : pods) {
            List<PodCondition> conditions = pod.getStatus()
                                               .getConditions();
            for (PodCondition condition : conditions) {
                if (Objects.equals(condition.getStatus(),
                                   CONDITION_STATUS_FALSE) &&
                    Objects.equals(condition.getReason(),
                                   POD_REASON_UNSCHEDULABLE)) {
                    return new MatchWithMsg(true,
                                            condition.getReason() + ", " +
                                            condition.getMessage());
                }
            }
        }
        return MatchWithMsg.NO_MATCH;
    }

    private MatchWithMsg imagePullBackOff(List<Pod> pods) {
        if (CollectionUtils.isEmpty(pods)) {
            return MatchWithMsg.NO_MATCH;
        }

        for (Pod pod : pods) {
            List<ContainerStatus> containerStatus =
                                  PodStatusUtil.getContainerStatus(pod);

            if (CollectionUtils.isNotEmpty(containerStatus)) {
                for (ContainerStatus status : containerStatus) {
                    ContainerState state = status.getState();
                    if (state != null) {
                        ContainerStateWaiting waiting = state.getWaiting();
                        if (waiting != null &&
                            IMAGE_PULL_BACKOFF.equals(waiting.getReason())) {
                            return new MatchWithMsg(true,
                                                    waiting.getReason() + ", " +
                                                    waiting.getMessage());
                        }
                    }
                }
            }
        }

        return MatchWithMsg.NO_MATCH;
    }

    private void recordFailedEvent(HugeGraphComputerJob computerJob,
                                   ComponentState masterJobState,
                                   ComponentState workerJobState) {
        StringBuilder builder = new StringBuilder();

        String masterFailedMsg = masterJobState.getMessage();
        if (StringUtils.isNotBlank(masterFailedMsg)) {
            builder.append("master failed message: \n");
            builder.append(masterFailedMsg);
        }

        String masterErrorLog = masterJobState.getErrorLog();
        if (StringUtils.isNotBlank(masterErrorLog)) {
            builder.append("\n");
            builder.append("master error log: \n");
            builder.append(masterErrorLog);
        }

        String workerFailedMsg = workerJobState.getMessage();
        if (StringUtils.isNotBlank(workerFailedMsg)) {
            builder.append("\n");
            builder.append("worker failed message: \n");
            builder.append(workerFailedMsg);
        }

        String workerErrorLog = workerJobState.getErrorLog();
        if (StringUtils.isNotBlank(workerErrorLog)) {
            builder.append("\n");
            builder.append("worker error log: \n");
            builder.append(workerErrorLog);
        }

        String crName = computerJob.getMetadata().getName();
        this.recordEvent(computerJob, EventType.WARNING,
                         KubeUtil.failedEventName(crName), "ComputerJobFailed",
                         builder.toString());
    }

    private String getErrorLog(List<Pod> pods) {
        for (Pod pod : pods) {
            String namespace = pod.getMetadata().getNamespace();
            String name = pod.getMetadata().getName();
            if (pod.getStatus() != null &&
                PodPhase.FAILED.value().equals(pod.getStatus().getPhase())) {

                KubernetesClient client = this.kubeClient;
                if (!Objects.equals(this.kubeClient.getNamespace(),
                                    namespace)) {
                    client = this.kubeClient.inNamespace(namespace);
                }

                String log = client.pods().withName(name)
                                   .tailingLines(ERROR_LOG_TAILING_LINES)
                                   .getLog(true);
                if (StringUtils.isNotBlank(log)) {
                    return log + "\n podName:" + pod.getMetadata().getName();
                }
            }
        }
        return StringUtils.EMPTY;
    }
}
