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
import java.util.Set;

import com.baidu.hugegraph.computer.k8s.Constants;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobSpec;
import com.baidu.hugegraph.computer.k8s.crd.model.HugeGraphComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.model.Protocol;
import com.baidu.hugegraph.computer.k8s.crd.model.ResourceName;
import com.baidu.hugegraph.computer.k8s.util.KubeUtil;
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
import io.fabric8.kubernetes.api.model.KubernetesList;
import io.fabric8.kubernetes.api.model.KubernetesListBuilder;
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
import io.fabric8.kubernetes.client.utils.Serialization;

public class ComputerJobDeployer {
    private final KubernetesClient kubeClient;

    public ComputerJobDeployer(KubernetesClient kubeClient) {
        this.kubeClient = kubeClient;
    }

    public void deploy(HugeGraphComputerJob computerJob,
                       ComputerJobComponent observed) {

        String namespace = computerJob.getMetadata().getNamespace();
        String name = computerJob.getMetadata().getName();
        ComputerJobSpec spec = computerJob.getSpec();

        Set<ContainerPort> ports = this.handleConfig(spec);

        ConfigMap configMap = this.desiredConfigMap(namespace, name, spec);
        Job masterJob = this.desiredMasterJob(namespace, name, spec, ports);
        Job workerJob = this.desiredWorkerJob(namespace, name, spec, ports);

        KubeUtil.setOwnerReference(configMap, masterJob, workerJob);

        KubernetesList resources = new KubernetesListBuilder()
                .addToItems(configMap, masterJob, workerJob)
                .build();

        this.kubeClient.resourceList(resources)
                       .inNamespace(computerJob.getMetadata().getNamespace())
                       .deletingExisting();

        this.kubeClient.resourceList(resources)
                       .inNamespace(computerJob.getMetadata().getNamespace())
                       .createOrReplace();
    }

    private ConfigMap desiredConfigMap(String namespace, String crName,
                                       ComputerJobSpec spec) {
        String component = "configMap";
        Map<String, String> computerConf = spec.getComputerConf();
        Map<String, String> data = new HashMap<>();
        data.put(Constants.COMPUTER_CONF_FILE,
                 Serialization.asYaml(computerConf));

        return new ConfigMapBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName(KubeUtil.configMapName(crName))
                .withLabels(this.geCommonLabels(crName, component))
                .endMetadata()
                .withData(data)
                .build();
    }

    private Set<ContainerPort> handleConfig(ComputerJobSpec spec) {
        Map<String, String> config = spec.getComputerConf();

        config.put(Constants.WORKER_COUNT_KEY,
                   String.valueOf(spec.getWorkerInstances()));

        config.put(Constants.TRANSPORT_IP_KEY,
                   String.format("${env:%s}", Constants.POD_IP));
        config.put(Constants.RPC_IP_KEY,
                   String.format("${env:%s}", Constants.POD_IP));

        String transportPort = config.computeIfAbsent(
                Constants.TRANSPORT_PORT_KEY,
                k -> String.valueOf(Constants.TRANSPORT_PORT));
        String rpcPort = config.computeIfAbsent(
                Constants.RPC_PORT_KEY,
                k -> String.valueOf(Constants.RPC_PORT));

        ContainerPort transportContainerPort = new ContainerPortBuilder()
                .withName(Constants.TRANSPORT_PORT_KEY)
                .withContainerPort(Integer.valueOf(transportPort))
                .withProtocol(Protocol.TCP.value())
                .build();
        ContainerPort rpcContainerPort = new ContainerPortBuilder()
                .withName(Constants.RPC_PORT_KEY)
                .withContainerPort(Integer.valueOf(rpcPort))
                .withProtocol(Protocol.TCP.value())
                .build();

        return Sets.newHashSet(transportContainerPort, rpcContainerPort);
    }

    public Job desiredMasterJob(String namespace, String crName,
                                ComputerJobSpec spec,
                                Set<ContainerPort> ports) {
        String component = "master";

        List<String> command = Lists.newArrayList("/bin/sh" , "-c");
        List<String> args = Lists.newArrayList("master.sh");

        Container container = this.getContainer(component, spec, ports,
                                                command, args);
        List<Container> containers = Lists.newArrayList(container);

        int instances = Constants.MASTER_INSTANCES;
        return this.getJob(namespace, crName, component, spec,
                           instances, containers);
    }

    public Job desiredWorkerJob(String namespace, String crName,
                                ComputerJobSpec spec,
                                Set<ContainerPort> ports) {
        String component = "worker";

        List<String> command = Lists.newArrayList("/bin/sh" , "-c");
        List<String> args = Lists.newArrayList("worker.sh");

        Container container = this.getContainer(component, spec, ports,
                                                command, args);
        List<Container> containers = Lists.newArrayList(container);

        int instances = spec.getWorkerInstances();
        return this.getJob(namespace, crName, component, spec,
                           instances, containers);
    }

    private Job getJob(String namespace, String crName,
                       String component, ComputerJobSpec spec,
                       int instances, List<Container> containers) {
        PodSpec podSpec = new PodSpecBuilder()
                .withContainers(containers)
                .withImagePullSecrets(spec.getPullSecrets())
                .withVolumes(this.getConfigVolume(crName))
                .build();

        return new JobBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName(KubeUtil.masterJobName(crName))
                .addToLabels(this.geCommonLabels(crName, component))
                .endMetadata()
                .withNewSpec()
                .withParallelism(instances)
                .withCompletions(instances)
                .withBackoffLimit(Constants.JOB_BACKOFF_LIMIT)
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(this.geCommonLabels(crName, component + "-pod"))
                .endMetadata()
                .withSpec(podSpec)
                .endTemplate()
                .endSpec()
                .build();
    }

    private Container getContainer(String name, ComputerJobSpec spec,
                                   Collection<ContainerPort> ports,
                                   Collection<String> command,
                                   Collection<String> args) {
        List<EnvVar> envVars = spec.getEnvVars();
        if (envVars == null) {
            envVars = new ArrayList<>();
        }

        // Add Pod info to env
        EnvVar podIP = new EnvVarBuilder()
                .withName(Constants.POD_IP)
                .withNewValueFrom()
                .withNewFieldRef()
                .withFieldPath(Constants.POD_IP_PATH)
                .endFieldRef()
                .endValueFrom()
                .build();
        envVars.add(podIP);
        EnvVar podName = new EnvVarBuilder()
                .withName(Constants.POD_NAME)
                .withNewValueFrom()
                .withNewFieldRef()
                .withFieldPath(Constants.POD_NAME_PATH)
                .endFieldRef()
                .endValueFrom()
                .build();
        envVars.add(podName);
        EnvVar podNamespace = new EnvVarBuilder()
                .withName(Constants.POD_NAMESPACE)
                .withNewValueFrom()
                .withNewFieldRef()
                .withFieldPath(Constants.POD_NAMESPACE_PATH)
                .endFieldRef()
                .endValueFrom()
                .build();
        envVars.add(podNamespace);

        Quantity masterCpu = spec.getMasterCpu();
        Quantity masterMemory = spec.getMasterMemory();
        VolumeMount configMount = this.getConfigMount();

        return new ContainerBuilder()
                .withName(name)
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getPullPolicy())
                .withEnv(spec.getEnvVars())
                .withEnvFrom(spec.getEnvFrom())
                .withWorkingDir(Constants.WORKING_DIR)
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
                .withName(Constants.CONFIG_MAP_VOLUME)
                .withMountPath(Constants.CONFIG_FILE_PATH)
                .build();
    }

    private Volume getConfigVolume(String configMapName) {
        return new VolumeBuilder()
                .withName(Constants.CONFIG_MAP_VOLUME)
                .withNewConfigMap()
                    .withName(configMapName)
                .endConfigMap()
                .build();
    }

    public Map<String, String> geCommonLabels(String crName, String component) {
        Map<String, String> labels = new HashMap<>();
        labels.put("app", HugeGraphComputerJob.KIND + "-" + crName);
        labels.put("component", component);
        return labels;
    }
}
