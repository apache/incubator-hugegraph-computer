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
import com.baidu.hugegraph.computer.k8s.crd.ComputerJobProvider;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.EventSource;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public class EventRecorder {

    private static final Logger LOG = Log.logger(EventRecorder.class);

    private static final String HOST_NAME_KEY = "host";
    private static final String OPERATOR_NAMESPACE_NAME = "default";
    private static final String REASON = "SuperStepStatChanged";

    private final KubernetesClient k8sClient;

    private static final Map<String, String> ENV_MAP;

    static {
        ENV_MAP = System.getenv();
    }

    public EventRecorder() {
        this.k8sClient = new DefaultKubernetesClient();
    }

    public void recordSuperStep(String jobId, JobState jobState) {
        String name = jobId + "-event";

        String message = String.format("%s\n%s\n%s", jobState.superstep(),
                                       jobState.maxSuperstep(),
                                       JsonUtil.toJson(
                                       jobState.lastSuperstepStat()));

        String now = OffsetDateTime.now().toString();

        EventSource eventSource = new EventSource();
        eventSource.setHost(ENV_MAP.getOrDefault(HOST_NAME_KEY,
                                                 "localhost"));
        eventSource.setComponent(ComputerJobProvider.SINGULAR + "-master");

        ObjectReference objectReference = new ObjectReferenceBuilder()
                .withApiVersion(ComputerJobProvider.API_VERSION)
                .withKind(ComputerJobProvider.KIND)
                .withName(jobId)
                .build();

        Event event = new EventBuilder()
                .withNewMetadata().withName(name).endMetadata()
                .withMessage(message)
                .withLastTimestamp(now)
                .withType(EventType.Normal.name())
                .withInvolvedObject(objectReference)
                .withSource(eventSource)
                .withReason(REASON)
                .build();

        LOG.info("Create a event for superStep: {}", event.toString());
        this.k8sClient.v1().events()
                      .inNamespace(OPERATOR_NAMESPACE_NAME)
                      .createOrReplace(event);

    }

    private enum EventType {

        Normal,
        Warning
    }
}
