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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.driver.config.ComputerOptions;
import com.baidu.hugegraph.computer.k8s.Constants;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobSpec;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.ResourceName;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;

public class ComputerJobDeployer {

    private static final Logger LOG = Log.logger(ComputerJobDeployer.class);

    private final NamespacedKubernetesClient kubeClient;

    // NO BACKOFF
    private static final int JOB_BACKOFF_LIMIT = 0;
    private static final String JOB_RESTART_POLICY = "Never";

    private static final String RANDOM_PORT = "0";
    private static final String PROTOCOL = "TCP";
    private static final String TRANSPORT_PORT_NAME = "transport-port";
    private static final String RPC_PORT_NAME = "rpc-port";
    private static final int DEFAULT_TRANSPORT_PORT = 8099;
    private static final int DEFAULT_RPC_PORT = 8090;

    private static final String CONFIG_MAP_VOLUME = "config-map-volume";

    private static final String POD_IP_KEY = "status.podIP";
    private static final String POD_NAMESPACE_KEY = "metadata.namespace";
    private static final String POD_NAME_KEY = "metadata.name";

    public ComputerJobDeployer(NamespacedKubernetesClient kubeClient) {
        this.kubeClient = kubeClient;
    }

    public void deploy(ComputerJobComponent observed) {
        HugeGraphComputerJob computerJob = observed.computerJob();

        Set<ContainerPort> ports = this.handleConfig(computerJob.getSpec());

        ComputerJobComponent desired = new ComputerJobComponent();
        desired.configMap(this.desiredConfigMap(computerJob));
        desired.masterJob(this.desiredMasterJob(computerJob, ports));
        desired.workerJob(this.desiredWorkerJob(computerJob, ports));

        this.reconcileComponent(computerJob.getMetadata().getNamespace(),
                                desired, observed);
    }

    private void reconcileComponent(String namespace,
                                    ComputerJobComponent desired,
                                    ComputerJobComponent observed) {
        ConfigMap desiredConfigMap = desired.configMap();
        ConfigMap observedConfigMap = observed.configMap();

        final KubernetesClient client;
        if (!Objects.equals(this.kubeClient.getNamespace(), namespace)) {
            client = this.kubeClient.inNamespace(namespace);
        } else {
            client = this.kubeClient;
        }

        if (desiredConfigMap == null && observedConfigMap != null) {
            client.configMaps().delete(observedConfigMap);
        } else if (desiredConfigMap != null && observedConfigMap == null) {
            KubeUtil.ignoreExists(() -> client.configMaps()
                                              .create(desiredConfigMap));
        }
        if (desiredConfigMap != null && observedConfigMap != null) {
            LOG.debug("ConfigMap already exists, no action");
        }

        Job desiredMasterJob = desired.masterJob();
        Job observedMasterJob = observed.masterJob();
        if (desiredMasterJob == null && observedMasterJob != null) {
            client.batch().v1().jobs().delete(observedMasterJob);
        } else if (desiredMasterJob != null && observedMasterJob == null) {
            KubeUtil.ignoreExists(() -> client.batch().v1().jobs()
                                              .create(desiredMasterJob));
        }
        if (desiredMasterJob != null && observedMasterJob != null) {
            LOG.debug("MasterJob already exists, no action");
        }

        Job desiredWorkerJob = desired.workerJob();
        Job observedWorkerJob = observed.workerJob();
        if (desiredWorkerJob == null && observedWorkerJob != null) {
            client.batch().v1().jobs().delete(observedWorkerJob);
        } else if (desiredWorkerJob != null && observedWorkerJob == null) {
            KubeUtil.ignoreExists(() -> client.batch().v1().jobs()
                                              .create(desiredWorkerJob));
        }
        if (desiredWorkerJob != null && observedWorkerJob != null) {
            LOG.debug("WorkerJob already exists, no action");
        }
    }

    private Set<ContainerPort> handleConfig(ComputerJobSpec spec) {
        Map<String, String> config = spec.getComputerConf();

        config.put(ComputerOptions.JOB_ID.name(),
                   String.valueOf(spec.getJobId()));

        config.put(ComputerOptions.JOB_WORKERS_COUNT.name(),
                   String.valueOf(spec.getWorkerInstances()));

        String ip = String.format("${%s}", Constants.ENV_POD_IP);
        config.put(ComputerOptions.TRANSPORT_SERVER_HOST.name(), ip);
        config.put(ComputerOptions.RPC_SERVER_HOST.name(), ip);

        String transportPort = config.get(
                               ComputerOptions.TRANSPORT_SERVER_PORT.name());
        if (StringUtils.isBlank(transportPort) ||
            RANDOM_PORT.equals(transportPort)) {
            transportPort = String.valueOf(DEFAULT_TRANSPORT_PORT);
            config.put(ComputerOptions.TRANSPORT_SERVER_PORT.name(),
                       transportPort);
        }

        String rpcPort = config.get(
                         ComputerOptions.RPC_SERVER_PORT.name());
        if (StringUtils.isBlank(rpcPort) || RANDOM_PORT.equals(rpcPort)) {
            rpcPort = String.valueOf(DEFAULT_RPC_PORT);
            config.put(ComputerOptions.RPC_SERVER_PORT.name(), rpcPort);
        }

        ContainerPort transportContainerPort = new ContainerPortBuilder()
                .withName(TRANSPORT_PORT_NAME)
                .withContainerPort(Integer.valueOf(transportPort))
                .withProtocol(PROTOCOL)
                .build();
        ContainerPort rpcContainerPort = new ContainerPortBuilder()
                .withName(RPC_PORT_NAME)
                .withContainerPort(Integer.valueOf(rpcPort))
                .withProtocol(PROTOCOL)
                .build();

        return Sets.newHashSet(transportContainerPort, rpcContainerPort);
    }

    private ConfigMap desiredConfigMap(HugeGraphComputerJob computerJob) {
        String crName = computerJob.getMetadata().getName();
        ComputerJobSpec spec = computerJob.getSpec();

        Map<String, String> computerConf = spec.getComputerConf();
        Map<String, String> data = new HashMap<>();
        data.put(Constants.COMPUTER_CONF_FILE,
                 KubeUtil.asProperties(computerConf));

        String name = KubeUtil.configMapName(crName);

        return new ConfigMapBuilder()
                .withMetadata(this.getMetadata(computerJob, name))
                .withData(data)
                .build();
    }

    public Job desiredMasterJob(HugeGraphComputerJob computerJob,
                                Set<ContainerPort> ports) {
        String crName = computerJob.getMetadata().getName();
        ComputerJobSpec spec = computerJob.getSpec();

        List<String> command = spec.getMasterCommand();
        if (CollectionUtils.isEmpty(command)) {
            command = Lists.newArrayList("/bin/sh", "-c");
        }
        List<String> args = spec.getMasterArgs();

        String name = KubeUtil.masterJobName(crName);

        ObjectMeta metadata = this.getMetadata(computerJob, name);

        Container container = this.getContainer(name, spec, ports,
                                                command, args);
        List<Container> containers = Lists.newArrayList(container);

        final int instances = Constants.MASTER_INSTANCES;
        return this.getJob(crName, metadata, spec, instances, containers);
    }

    public Job desiredWorkerJob(HugeGraphComputerJob computerJob,
                                Set<ContainerPort> ports) {
        String crName = computerJob.getMetadata().getName();
        ComputerJobSpec spec = computerJob.getSpec();

        List<String> command = spec.getWorkerCommand();
        if (CollectionUtils.isEmpty(command)) {
            command = Lists.newArrayList("/bin/sh", "-c");
        }
        List<String> args = spec.getWorkerArgs();

        String name = KubeUtil.workerJobName(crName);

        ObjectMeta metadata = this.getMetadata(computerJob, name);

        Container container = this.getContainer(name, spec, ports,
                                                command, args);
        List<Container> containers = Lists.newArrayList(container);

        final int instances = spec.getWorkerInstances();
        return this.getJob(crName, metadata, spec, instances, containers);
    }

    private Job getJob(String crName, ObjectMeta meta, ComputerJobSpec spec,
                       int instances, List<Container> containers) {

        String configMapName = KubeUtil.configMapName(crName);
        Volume configVolume = this.getConfigVolume(configMapName);

        PodSpec podSpec = new PodSpecBuilder()
                .withContainers(containers)
                .withImagePullSecrets(spec.getPullSecrets())
                .withRestartPolicy(JOB_RESTART_POLICY)
                .withVolumes(configVolume)
                .build();

        return new JobBuilder().withMetadata(meta)
                               .withNewSpec()
                               .withParallelism(instances)
                               .withCompletions(instances)
                               .withBackoffLimit(JOB_BACKOFF_LIMIT)
                               .withNewTemplate()
                                    .withNewMetadata()
                                        .withLabels(meta.getLabels())
                                    .endMetadata()
                                    .withSpec(podSpec)
                               .endTemplate()
                               .endSpec()
                               .build();
    }

    private Container getContainer(String name, ComputerJobSpec spec,
                                   Set<ContainerPort> ports,
                                   Collection<String> command,
                                   Collection<String> args) {
        List<EnvVar> envVars = spec.getEnvVars();
        if (envVars == null) {
            envVars = new ArrayList<>();
        }

        // Add Pod info to env
        EnvVar podIP = new EnvVarBuilder()
                .withName(Constants.ENV_POD_IP)
                .withNewValueFrom()
                .withNewFieldRef()
                .withFieldPath(POD_IP_KEY)
                .endFieldRef()
                .endValueFrom()
                .build();
        envVars.add(podIP);

        EnvVar podName = new EnvVarBuilder()
                .withName(Constants.ENV_POD_NAME)
                .withNewValueFrom()
                .withNewFieldRef()
                .withFieldPath(POD_NAME_KEY)
                .endFieldRef()
                .endValueFrom()
                .build();
        envVars.add(podName);

        EnvVar podNamespace = new EnvVarBuilder()
                .withName(Constants.ENV_POD_NAMESPACE)
                .withNewValueFrom()
                .withNewFieldRef()
                .withFieldPath(POD_NAMESPACE_KEY)
                .endFieldRef()
                .endValueFrom()
                .build();
        envVars.add(podNamespace);

        EnvVar confPath = new EnvVarBuilder()
                .withName(Constants.ENV_CONFIG_DIR)
                .withValue(Constants.CONFIG_PATH)
                .build();
        envVars.add(confPath);

        Quantity masterCpu = spec.getMasterCpu();
        Quantity masterMemory = spec.getMasterMemory();
        VolumeMount configMount = this.getConfigMount();

        return new ContainerBuilder()
                .withName(KubeUtil.containerName(name))
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getPullPolicy())
                .withEnv(spec.getEnvVars())
                .withEnvFrom(spec.getEnvFrom())
                .addToVolumeMounts(configMount)
                .addAllToCommand(command)
                .addAllToArgs(args)
                .addAllToPorts(ports)
                .withNewResources()
                    .addToLimits(ResourceName.CPU.value(), masterCpu)
                    .addToLimits(ResourceName.MEMORY.value(), masterMemory)
                .endResources()
                .build();
    }

    private VolumeMount getConfigMount() {
        return new VolumeMountBuilder()
                .withName(CONFIG_MAP_VOLUME)
                .withMountPath(Constants.CONFIG_PATH)
                .build();
    }

    private Volume getConfigVolume(String configMapName) {
        return new VolumeBuilder()
                .withName(CONFIG_MAP_VOLUME)
                .withNewConfigMap()
                    .withName(configMapName)
                .endConfigMap()
                .build();
    }

    public ObjectMeta getMetadata(HugeGraphComputerJob computerJob,
                                  String component) {
        String namespace = computerJob.getMetadata().getNamespace();
        String crName = computerJob.getMetadata().getName();

        OwnerReference ownerReference = new OwnerReferenceBuilder()
                .withName(crName)
                .withApiVersion(computerJob.getApiVersion())
                .withUid(computerJob.getMetadata().getUid())
                .withKind(computerJob.getKind())
                .withController(true)
                .withBlockOwnerDeletion(true)
                .build();

        String kind = HasMetadata.getKind(HugeGraphComputerJob.class);
        Map<String, String> labels = KubeUtil.commonLabels(kind, crName,
                                                           component);
        return new ObjectMetaBuilder().withNamespace(namespace)
                                      .withName(component)
                                      .addToLabels(labels)
                                      .withOwnerReferences(ownerReference)
                                      .build();
    }
}
