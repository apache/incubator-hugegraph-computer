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

package com.baidu.hugegraph.computer.k8s.event;

import java.time.OffsetDateTime;
import java.util.Map;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.driver.JobState;
import com.baidu.hugegraph.computer.k8s.crd.ComputerJob;
import com.baidu.hugegraph.computer.k8s.crd.ComputerJobList;
import com.baidu.hugegraph.computer.k8s.crd.model.ComputerJobState;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.EventSource;
import io.fabric8.kubernetes.api.model.EventSourceBuilder;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

public class EventRecorder {

    private static final Logger LOG = Log.logger(EventRecorder.class);

    private static final String HOST_NAME_KEY = "hostname";
    private static final String REASON = "SuperStepStatChanged";

    private final KubernetesClient kubeClient;
    private final MixedOperation<ComputerJob, ComputerJobList,
                                 Resource<ComputerJob>> computerJonClient;

    private static final Map<String, String> ENV_MAP;

    static {
        ENV_MAP = System.getenv();
    }

    public EventRecorder() {
        this.kubeClient = new DefaultKubernetesClient();
        this.computerJonClient = this.kubeClient.customResources(
                                                 ComputerJob.class,
                                                 ComputerJobList.class);
    }

    public void recordSuperStep(String jobId, JobState jobState) {
        String name = jobId + "-superstep-event";

        String namespace = this.kubeClient.getNamespace();
        if (namespace.isEmpty()) {
            namespace = "default";
        }

        ComputerJobState computerJobState = new ComputerJobState()
                .withSuperstep(jobState.superstep())
                .withMaxSuperstep(jobState.maxSuperstep())
                .withLastSuperstepStat(
                 JsonUtil.toJson(jobState.lastSuperstepStat()));

        String message = JsonUtil.toJson(computerJobState);

        ComputerJob computerJob = this.computerJonClient.inNamespace(namespace)
                                                        .withName(jobId)
                                                        .get();
        if (computerJob == null) {
            LOG.warn("The ComputerJob Resource don't exists");
            return;
        }

        ObjectReference objectReference = new ObjectReferenceBuilder()
                .withKind(computerJob.getKind())
                .withName(computerJob.getMetadata().getName())
                .withUid(computerJob.getMetadata().getUid())
                .withApiVersion(ComputerJob.API_VERSION)
                .withNamespace(namespace)
                .build();

        OwnerReference ownerReference = new OwnerReferenceBuilder()
                .withKind(computerJob.getKind())
                .withName(computerJob.getMetadata().getName())
                .withUid(computerJob.getMetadata().getUid())
                .withController(true)
                .withBlockOwnerDeletion(false)
                .build();

        EventSource eventSource = new EventSourceBuilder()
                .withHost(ENV_MAP.getOrDefault(HOST_NAME_KEY, "localhost"))
                .withComponent(ComputerJob.SINGULAR + "-master")
                .build();

        String now = OffsetDateTime.now().toString();

        Event event = new EventBuilder()
                .withNewMetadata().withName(name).endMetadata()
                .withMessage(message)
                .withLastTimestamp(now)
                .withType(EventType.Normal.name())
                .withInvolvedObject(objectReference)
                .withSource(eventSource)
                .withReason(REASON)
                .withNewMetadata()
                    .withOwnerReferences(ownerReference)
                    .endMetadata()
                .build();

        LOG.debug("Create a event for superStep: {}", event.toString());
        this.kubeClient.v1().events()
                       .inNamespace(namespace)
                       .createOrReplace(event);

    }

    private enum EventType {

        Normal,
        Warning
    }
}
