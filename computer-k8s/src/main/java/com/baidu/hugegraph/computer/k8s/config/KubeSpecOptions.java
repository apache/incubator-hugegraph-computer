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

package com.baidu.hugegraph.computer.k8s.config;

import static com.baidu.hugegraph.config.OptionChecker.allowValues;
import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import java.util.Map;

import com.baidu.hugegraph.computer.driver.config.WithoutDefaultConfigOption;
import com.baidu.hugegraph.config.ConfigListOption;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;
import com.google.common.collect.ImmutableMap;

import io.fabric8.kubernetes.api.model.Quantity;

public class KubeSpecOptions extends OptionHolder {

    private KubeSpecOptions() {
        super();
    }

    private static volatile KubeSpecOptions INSTANCE;

    public static synchronized KubeSpecOptions instance() {
        if (INSTANCE == null) {
            INSTANCE = new KubeSpecOptions();
            // Should initialize all static members first, then register.
            INSTANCE.registerOptions();
        }
        return INSTANCE;
    }

    public static boolean checkQuantity(String value) {
        try {
            Quantity.parse(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static final WithoutDefaultConfigOption<String> MASTER_CPU =
            new WithoutDefaultConfigOption<>(
                    "k8s.master_cpu",
                    "The value is cpu limit of master, it's units should " +
                    "meet the constraints of k8s for cpu units.",
                    KubeSpecOptions::checkQuantity,
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> MASTER_MEMORY =
            new WithoutDefaultConfigOption<>(
                    "k8s.master_memory",
                    "The value is memory limit of master, it's units should " +
                    "meet the constraints of k8s for memory units.",
                    KubeSpecOptions::checkQuantity,
                    String.class
            );

    public static final ConfigListOption<String> MASTER_COMMAND =
            new ConfigListOption<>(
                    "k8s.master_command",
                    "The value is run command of master, equivalent to " +
                    "'Entrypoint' field of Docker.",
                    disallowEmpty(),
                    "/bin/sh", "-c"
            );

    public static final ConfigListOption<String> MASTER_ARGS =
            new ConfigListOption<>(
                    "k8s.master_args",
                    "The value is run args of master, equivalent to 'Cmd' " +
                    "field of Docker.",
                    disallowEmpty(),
                    "echo master"
            );

    public static final WithoutDefaultConfigOption<String> WORKER_CPU =
            new WithoutDefaultConfigOption<>(
                    "k8s.worker_cpu",
                    "The value is cpu limit of worker, it's units should " +
                    "meet the constraints of k8s for cpu units.",
                    KubeSpecOptions::checkQuantity,
                    String.class
            );

    public static final WithoutDefaultConfigOption<String> WORKER_MEMORY =
            new WithoutDefaultConfigOption<>(
                    "k8s.worker_memory",
                    "The value is memory limit of worker, it's units should " +
                    "meet the constraints of k8s for memory units.",
                    KubeSpecOptions::checkQuantity,
                    String.class
            );

    public static final ConfigListOption<String> WORKER_COMMAND =
            new ConfigListOption<>(
                    "k8s.worker_command",
                    "The value is run command of worker, equivalent to " +
                    "'Entrypoint' field of Docker.",
                    disallowEmpty(),
                    "/bin/sh", "-c"
            );

    public static final ConfigListOption<String> WORKER_ARGS =
            new ConfigListOption<>(
                    "k8s.worker_args",
                    "The value is run args of worker, equivalent to 'Cmd' " +
                    "field of Docker.",
                    disallowEmpty(),
                    "echo worker"
            );

    public static final ConfigOption<Integer> WORKER_INSTANCES =
            new ConfigOption<>(
                    "k8s.worker_instances",
                    "The value is number of worker instances, it will " +
                    "override the 'job.workers_count' option.",
                    rangeInt(1, Integer.MAX_VALUE),
                    1
            );

    public static final ConfigOption<String> PULL_POLICY =
            new ConfigOption<>(
                    "k8s.pull_policy",
                    "The value is pull-policy of image.",
                    allowValues("Always", "Never", "IfNotPresent"),
                    "Always"
            );

    public static Map<String, ConfigOption<?>> ALLOW_USER_SETTINGS =
            new ImmutableMap.Builder<String, ConfigOption<?>>()
                    .put(MASTER_CPU.name(), MASTER_CPU)
                    .put(WORKER_CPU.name(), WORKER_CPU)
                    .put(MASTER_MEMORY.name(), MASTER_MEMORY)
                    .put(WORKER_MEMORY.name(), WORKER_MEMORY)
                    .put(WORKER_INSTANCES.name(), WORKER_INSTANCES)
                    .build();
}
