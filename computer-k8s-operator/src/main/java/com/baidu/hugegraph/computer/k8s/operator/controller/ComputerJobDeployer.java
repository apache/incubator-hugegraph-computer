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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.driver.config.ComputerOptions;
import com.baidu.hugegraph.computer.k8s.Constants;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobSpec;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.ResourceName;
import com.baidu.hugegraph.computer.k8s.operator.config.OperatorOptions;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
import com.baidu.hugegraph.config.HugeConfig;
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
    private static final int DEFAULT_TRANSPORT_THREADS = 8;
    private static final String COMPUTER_CONFIG_MAP_VOLUME =
            "computer-config-map-volume";

    private static final String CONTAINER_CPU_LIMIT_KEY = "limits.cpu";
    private static final String POD_IP_KEY = "status.podIP";
    private static final String POD_NAMESPACE_KEY = "metadata.namespace";
    private static final String POD_NAME_KEY = "metadata.name";

    private static final Long TERMINATION_GRACE_PERIOD = 180L;

    private final String internalEtcdUrl;
    private final String timezone;

    public ComputerJobDeployer(NamespacedKubernetesClient kubeClient,
                               HugeConfig config) {
        this.kubeClient = kubeClient;
        this.internalEtcdUrl = config.get(OperatorOptions.INTERNAL_ETCD_URL);
        this.timezone = config.get(OperatorOptions.TIMEZONE);
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
        config.putIfAbsent(ComputerOptions.BSP_ETCD_ENDPOINTS.name(),
                           this.internalEtcdUrl);

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

        /*
        Set a default number of transport threads,
        if the number of CPU quantity of the worker is not specified
         */
        if (spec.getWorkerCpu() == null) {
            String defaultThreads = String.valueOf(DEFAULT_TRANSPORT_THREADS);

            config.putIfAbsent(ComputerOptions.TRANSPORT_CLIENT_THREADS.name(),
                               defaultThreads);
            config.putIfAbsent(ComputerOptions.TRANSPORT_SERVER_THREADS.name(),
                               defaultThreads);
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

        String log4jXml = spec.getLog4jXml();
        if (StringUtils.isNotBlank(log4jXml)) {
            data.put(Constants.LOG_XML_FILE, log4jXml);
        }

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
            command = Constants.COMMAND;
        }
        List<String> args = spec.getMasterArgs();
        if (CollectionUtils.isEmpty(args)) {
            args = Constants.MASTER_ARGS;
        }

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
            command = Constants.COMMAND;
        }
        List<String> args = spec.getWorkerArgs();
        if (CollectionUtils.isEmpty(args)) {
            args = Constants.WORKER_ARGS;
        }

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
        List<Volume> volumes = spec.getVolumes();
        if (volumes == null) {
            volumes = new ArrayList<>();
        } else {
            volumes = Lists.newArrayList(volumes);
        }
        volumes.addAll(this.getConfigMapAndSecretVolumes(spec));

        String configMapName = KubeUtil.configMapName(crName);
        Volume configVolume = this.getComputerConfigVolume(configMapName);
        volumes.add(configVolume);

        PodSpec podSpec = new PodSpecBuilder()
                .withContainers(containers)
                .withImagePullSecrets(spec.getPullSecrets())
                .withRestartPolicy(JOB_RESTART_POLICY)
                .withTerminationGracePeriodSeconds(TERMINATION_GRACE_PERIOD)
                .withVolumes(volumes)
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

    private List<Volume> getConfigMapAndSecretVolumes(ComputerJobSpec spec) {
        List<Volume> volumes = new ArrayList<>();

        Map<String, String> configMapPaths = spec.getConfigMapPaths();
        if (MapUtils.isNotEmpty(configMapPaths)) {
            Set<String> configMapNames = configMapPaths.keySet();
            for (String configMapName : configMapNames) {
                Volume volume = new VolumeBuilder()
                        .withName(this.volumeName(configMapName))
                        .withNewConfigMap()
                        .withName(configMapName)
                        .endConfigMap()
                        .build();
                volumes.add(volume);
            }
        }

        Map<String, String> secretPaths = spec.getSecretPaths();
        if (MapUtils.isNotEmpty(secretPaths)) {
            Set<String> secretNames = secretPaths.keySet();
            for (String secretName : secretNames) {
                Volume volume = new VolumeBuilder()
                        .withName(this.volumeName(secretName))
                        .withNewSecret()
                        .withSecretName(secretName)
                        .endSecret()
                        .build();
                volumes.add(volume);
            }
        }

        return volumes;
    }

    private Container getContainer(String name, ComputerJobSpec spec,
                                   Set<ContainerPort> ports,
                                   Collection<String> command,
                                   Collection<String> args) {
        List<EnvVar> envVars = spec.getEnvVars();
        if (envVars == null) {
            envVars = new ArrayList<>();
        }

        // Add env
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

        EnvVar confDir = new EnvVarBuilder()
                .withName(Constants.ENV_CONFIG_DIR)
                .withValue(Constants.CONFIG_DIR)
                .build();
        envVars.add(confDir);

        EnvVar confPath = new EnvVarBuilder()
                .withName(Constants.ENV_COMPUTER_CONF_PATH)
                .withValue(Constants.COMPUTER_CONF_PATH)
                .build();
        envVars.add(confPath);

        String log4jXml = spec.getLog4jXml();
        if (StringUtils.isNotBlank(log4jXml)) {
            EnvVar logConfEnv = new EnvVarBuilder()
                    .withName(Constants.ENV_LOG4J_CONF_PATH)
                    .withValue(Constants.LOG_XML_PATH)
                    .build();
            envVars.add(logConfEnv);
        }

        String jarFile = spec.getJarFile();
        if (StringUtils.isNotBlank(jarFile)) {
            EnvVar jarFilePath = new EnvVarBuilder()
                    .withName(Constants.ENV_JAR_FILE_PATH)
                    .withValue(jarFile)
                    .build();
            envVars.add(jarFilePath);
        }

        String remoteJarUri = spec.getRemoteJarUri();
        if (StringUtils.isNotBlank(remoteJarUri)) {
            EnvVar jobJarURI = new EnvVarBuilder()
                    .withName(Constants.ENV_JOB_JAR_URI)
                    .withValue(remoteJarUri)
                    .build();
            envVars.add(jobJarURI);
        }

        String jvmOptions = spec.getJvmOptions();
        StringBuilder jvmOptionsBuilder = jvmOptions == null ?
                                          new StringBuilder() :
                                          new StringBuilder(jvmOptions.trim());
        jvmOptionsBuilder.append(" ").append("-Duser.timezone=")
                         .append(this.timezone);
        EnvVar jvmOptionsEnv = new EnvVarBuilder()
                .withName(Constants.ENV_JVM_OPTIONS)
                .withValue(jvmOptionsBuilder.toString().trim())
                .build();
        envVars.add(jvmOptionsEnv);

        Quantity cpu;
        Quantity memory;
        if (name.contains("master")) {
            cpu = spec.getMasterCpu();
            memory = spec.getMasterMemory();
        } else {
            cpu = spec.getWorkerCpu();
            memory = spec.getWorkerMemory();
        }

        if (cpu != null) {
            EnvVar cpuLimit = new EnvVarBuilder()
                    .withName(Constants.ENV_CPU_LIMIT)
                    .withValue(cpu.toString())
                    .build();
            envVars.add(cpuLimit);
        }

        if (memory != null) {
            EnvVar memoryLimit = new EnvVarBuilder()
                    .withName(Constants.ENV_MEMORY_LIMIT)
                    .withValue(memory.toString())
                    .build();
            envVars.add(memoryLimit);
        }

        List<VolumeMount> volumeMounts = spec.getVolumeMounts();
        if (volumeMounts == null) {
            volumeMounts = new ArrayList<>();
        } else {
            volumeMounts = Lists.newArrayList(volumeMounts);
        }

        // Mount configmap and secret files
        Map<String, String> paths = new HashMap<>();
        Map<String, String> configMapPaths = spec.getConfigMapPaths();
        if (MapUtils.isNotEmpty(configMapPaths)) {
            paths.putAll(configMapPaths);
        }
        Map<String, String> secretPaths = spec.getSecretPaths();
        if (MapUtils.isNotEmpty(secretPaths)) {
            paths.putAll(secretPaths);
        }
        Set<Map.Entry<String, String>> entries = paths.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            VolumeMount volumeMount = new VolumeMountBuilder()
                    .withName(this.volumeName(entry.getKey()))
                    .withMountPath(entry.getValue())
                    .withReadOnly(true)
                    .build();
            volumeMounts.add(volumeMount);
        }

        VolumeMount configMount = this.getComputerConfigMount();
        volumeMounts.add(configMount);

        return new ContainerBuilder()
                .withName(KubeUtil.containerName(name))
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getPullPolicy())
                .withEnv(spec.getEnvVars())
                .withEnvFrom(spec.getEnvFrom())
                .withVolumeMounts(volumeMounts)
                .addAllToCommand(command)
                .addAllToArgs(args)
                .addAllToPorts(ports)
                .withNewResources()
                    .addToLimits(ResourceName.CPU.value(), cpu)
                    .addToLimits(ResourceName.MEMORY.value(), memory)
                .endResources()
                .build();
    }

    private VolumeMount getComputerConfigMount() {
        return new VolumeMountBuilder()
                .withName(COMPUTER_CONFIG_MAP_VOLUME)
                .withMountPath(Constants.CONFIG_DIR)
                .build();
    }

    private Volume getComputerConfigVolume(String configMapName) {
        return new VolumeBuilder()
                .withName(COMPUTER_CONFIG_MAP_VOLUME)
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

    private String volumeName(String name) {
        return name + "-volume";
    }
}
