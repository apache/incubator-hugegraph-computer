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
import java.util.Objects;

import com.baidu.hugegraph.computer.driver.config.NoDefaultConfigOption;
import com.baidu.hugegraph.computer.k8s.Constants;
import com.baidu.hugegraph.config.ConfigListOption;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;
import com.google.common.collect.ImmutableMap;

import io.fabric8.kubernetes.api.model.Quantity;

public class KubeDriverOptions extends OptionHolder {

    private KubeDriverOptions() {
        super();
    }

    private static volatile KubeDriverOptions INSTANCE;

    public static synchronized KubeDriverOptions instance() {
        if (INSTANCE == null) {
            INSTANCE = new KubeDriverOptions();
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

    public static final ConfigOption<String> NAMESPACE =
            new ConfigOption<>(
                    "k8s.namespace",
                    "The value is namespace of hugegraph-computer system.",
                    disallowEmpty(),
                    Constants.DEFAULT_NAMESPACE
            );

    public static final ConfigOption<String> IMAGE_REPOSITORY_REGISTRY =
            new ConfigOption<>(
                    "k8s.image_repository_registry",
                    "The value is address for login image repository.",
                    Objects::nonNull,
                    "registry.hub.docker.com"
            );

    public static final ConfigOption<String> IMAGE_REPOSITORY_USERNAME =
            new ConfigOption<>(
                    "k8s.image_repository_username",
                    "The value is username for login image repository.",
                    disallowEmpty(),
                    "username"
            );

    public static final ConfigOption<String> IMAGE_REPOSITORY_PASSWORD =
            new ConfigOption<>(
                    "k8s.image_repository_password",
                    "The value is password for login image repository.",
                    disallowEmpty(),
                    "password"
            );

    public static final ConfigOption<String> IMAGE_REPOSITORY_URL =
            new ConfigOption<>(
                    "k8s.image_repository_url",
                    "The value is url of image repository.",
                    disallowEmpty(),
                    "hugegraph"
            );

    public static final ConfigOption<String> BUILD_IMAGE_SHELL =
            new ConfigOption<>(
                    "k8s.build_image_shell",
                    "The value is shell path to build image.",
                    disallowEmpty(),
                    "conf/images/upload.sh"
            );

    public static final ConfigListOption<String> PULL_SECRET_NAMES =
            new ConfigListOption<>(
                    "k8s.pull_secret_names",
                    "The value is pullSecret name list of image",
                    null,
                    ""
            );

    public static final NoDefaultConfigOption<String> MASTER_CPU =
            new NoDefaultConfigOption<>(
                    "k8s.master_cpu",
                    "The value is cpu limit of master.",
                    KubeDriverOptions::checkQuantity,
                    String.class
            );

    public static final NoDefaultConfigOption<String> MASTER_MEMORY =
            new NoDefaultConfigOption<>(
                    "k8s.master_memory",
                    "The value is memory limit of master.",
                    KubeDriverOptions::checkQuantity,
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

    public static final NoDefaultConfigOption<String> WORKER_CPU =
            new NoDefaultConfigOption<>(
                    "k8s.worker_cpu",
                    "The value is cpu limit of worker.",
                    KubeDriverOptions::checkQuantity,
                    String.class
            );

    public static final NoDefaultConfigOption<String> WORKER_MEMORY =
            new NoDefaultConfigOption<>(
                    "k8s.worker_memory",
                    "The value is memory limit of worker.",
                    KubeDriverOptions::checkQuantity,
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
                    "override job.workers_count option.",
                    rangeInt(1, Integer.MAX_VALUE),
                    1
            );

    public static final ConfigOption<String> PULL_POLICY =
            new ConfigOption<>(
                    "k8s.pull_policy",
                    "The value is pull policy of image.",
                    allowValues("Always", "Never", "IfNotPresent"),
                    "Always"
            );

    public static Map<String, ConfigOption<?>> ALLOW_USER_SETTINGS =
            ImmutableMap.of(
            MASTER_CPU.name(), MASTER_CPU,
            WORKER_CPU.name(), WORKER_CPU,
            MASTER_MEMORY.name(), MASTER_MEMORY,
            WORKER_MEMORY.name(), WORKER_MEMORY,
            WORKER_INSTANCES.name(), WORKER_INSTANCES
    );
}
