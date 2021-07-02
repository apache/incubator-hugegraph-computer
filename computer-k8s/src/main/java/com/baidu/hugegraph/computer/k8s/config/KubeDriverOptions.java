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

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;

import com.baidu.hugegraph.computer.k8s.Constants;
import com.baidu.hugegraph.config.ConfigListOption;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;

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

    public static final ConfigOption<String> NAMESPACE =
            new ConfigOption<>(
                    "k8s.namespace",
                    "The value is namespace of hugegraph-computer system.",
                    disallowEmpty(),
                    Constants.DEFAULT_NAMESPACE
            );

    public static final ConfigOption<String> KUBE_CONFIG =
            new ConfigOption<>(
                    "k8s.kube_config",
                    "The value is path of k8s config file.",
                    disallowEmpty(),
                    System.getProperty("user.home") + "/.kube/config"
            );

    public static final ConfigOption<String> IMAGE_REPOSITORY_REGISTRY =
            new ConfigOption<>(
                    "k8s.image_repository_registry",
                    "The value is address for login image repository.",
                    null,
                    ""
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
                    "hugegraph/computer"
            );

    public static final ConfigOption<String> BUILD_IMAGE_BASH_PATH =
            new ConfigOption<>(
                    "k8s.build_image_shell_path",
                    "The value is bash path to build image.",
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
}
