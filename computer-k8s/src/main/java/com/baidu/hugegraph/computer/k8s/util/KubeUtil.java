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

package com.baidu.hugegraph.computer.k8s.util;

import java.net.HttpURLConnection;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.k8s.Constants;
import com.baidu.hugegraph.computer.k8s.crd.model.EventType;
import com.baidu.hugegraph.util.Log;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.EventSource;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.utils.Utils;

public class KubeUtil {

    private static final Logger LOG = Log.logger(KubeUtil.class);

    private static final Pattern LINE_PATTERN = Pattern.compile("_([a-z])");

    /**
     * Tries a condition func until the initial delay specified.
     *
     * @param initialDelay the initial delay
     * @param interval     the check interval
     * @param timeout      the timeout period
     * @param condition    the condition
     * @return returns true if gracefully finished
     */
    public static boolean waitUntilReady(Duration initialDelay,
                                         Duration interval,
                                         Duration timeout,
                                         Supplier<Boolean> condition,
                                         ScheduledExecutorService executor) {
        AtomicBoolean result = new AtomicBoolean(false);
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        ScheduledFuture<?> future = executor.scheduleAtFixedRate(
                                    () -> {
                                        try {
                                            result.set(condition.get());
                                        } catch (Exception e) {
                                            result.set(false);
                                        }
                                    },
                                    initialDelay.toMillis(),
                                    interval.toMillis(),
                                    TimeUnit.MILLISECONDS);
        try {
            while (System.currentTimeMillis() < deadline) {
                if (result.get()) {
                    return true;
                }
            }
        } finally {
            future.cancel(true);
        }
        return result.get();
    }

    public static String crName(String jobId) {
        return jobId.toLowerCase();
    }

    public static String genJobId(String algorithmName) {
        return algorithmName + "-" + Utils.randomString(10);
    }

    public static String imageName(String repository, String algorithmName,
                                   String version) {
        if (StringUtils.isBlank(version)) {
            version = "latest";
        }
        return String.format("%s:%s-%s", repository, algorithmName, version);
    }

    public static String masterJobName(String crName) {
        return crName + "-master";
    }

    public static String workerJobName(String crName) {
        return crName + "-worker";
    }

    public static String configMapName(String crName) {
        return crName + "-configmap";
    }

    public static String failedEventName(String crName) {
        return crName + "-failedEvent";
    }

    public static String now() {
        return OffsetDateTime.now().toString();
    }

    public static int intVal(Integer integer) {
        return integer != null ? integer : -1;
    }

    public static String asProperties(Map<String, String> map) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isNotBlank(key)) {
                builder.append(key);
                builder.append("=");
                if (value != null) {
                    builder.append(value);
                }
                builder.append("\n");
            }
        }
        return builder.toString();
    }

    public static <T> T ignoreExists(Supplier<T> supplier) {
        try {
            return supplier.get();
        } catch (KubernetesClientException exception) {
            if (exception.getCode() == HttpURLConnection.HTTP_CONFLICT) {
                LOG.warn("Resource already exists, message: {}",
                         exception.getMessage());
            } else {
                throw exception;
            }
        }
        return null;
    }

    public static Map<String, String> commonLabels(String kind,
                                                   String crName,
                                                   String component) {
        Map<String, String> labels = new HashMap<>();
        labels.put("app", kind);
        labels.put("resourceName", crName);
        labels.put("component", component);
        return labels;
    }

    public static String matchKindAndGetCrName(Map<String, String> labels,
                                               String kind) {
        String app = labels.get("app");
        String crName = labels.get("resourceName");
        if (Objects.equals(kind, app) && StringUtils.isNotBlank(crName)) {
            return crName;
        }

        return StringUtils.EMPTY;
    }

    public static Event buildEvent(HasMetadata eventRef,
                                   EventSource eventSource,
                                   EventType eventType, String eventName,
                                   String reason, String message) {
        String namespace = eventRef.getMetadata().getNamespace();
        String eventRefName = eventRef.getMetadata().getName();
        ObjectReference objectReference = new ObjectReferenceBuilder()
                .withKind(eventRef.getKind())
                .withNamespace(namespace)
                .withName(eventRefName)
                .withUid(eventRef.getMetadata().getUid())
                .withApiVersion(HasMetadata.getApiVersion(eventRef.getClass()))
                .build();

        return new EventBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName(eventName)
                .endMetadata()
                .withMessage(message)
                .withLastTimestamp(KubeUtil.now())
                .withType(eventType.value())
                .withInvolvedObject(objectReference)
                .withSource(eventSource)
                .withReason(reason)
                .build();
    }

    /**
     * Convert config key to spec key.
     * eg. "k8s.master_cpu" -> "masterCpu"
     */
    public static String covertSpecKey(String configKey) {
        configKey = configKey.substring(Constants.K8S_SPEC_PREFIX.length());
        Matcher matcher = LINE_PATTERN.matcher(configKey);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, matcher.group(1).toUpperCase());
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
