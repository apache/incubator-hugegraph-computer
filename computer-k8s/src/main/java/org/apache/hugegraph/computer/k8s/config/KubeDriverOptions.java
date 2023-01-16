/*
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

package org.apache.hugegraph.computer.k8s.config;

import static org.apache.hugegraph.config.OptionChecker.allowValues;
import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.k8s.Constants;
import org.apache.hugegraph.config.ConfigListOption;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

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
                    "The namespace of hugegraph-computer system.",
                    disallowEmpty(),
                    Constants.DEFAULT_NAMESPACE
            );

    public static final ConfigOption<String> KUBE_CONFIG =
            new ConfigOption<>(
                    "k8s.kube_config",
                    "The path of k8s config file.",
                    disallowEmpty(),
                    FileUtils.getUserDirectoryPath() + "/.kube/config"
            );

    public static final ConfigOption<String> FRAMEWORK_IMAGE_URL =
            new ConfigOption<>(
                    "k8s.framework_image_url",
                    "The image url of computer framework.",
                    disallowEmpty(),
                    "hugegraph/hugegraph-computer:latest"
            );

    public static final ConfigOption<String> BUILD_IMAGE_BASH_PATH =
            new ConfigOption<>(
                    "k8s.build_image_bash_path",
                    "The path of command used to build image.",
                    null,
                    ""
            );

    public static final ConfigOption<String> IMAGE_REPOSITORY_REGISTRY =
            new ConfigOption<>(
                    "k8s.image_repository_registry",
                    "The address for login image repository.",
                    null,
                    ""
            );

    public static final ConfigOption<String> IMAGE_REPOSITORY_USERNAME =
            new ConfigOption<>(
                    "k8s.image_repository_username",
                    "The username for login image repository.",
                    null,
                    ""
            );

    public static final ConfigOption<String> IMAGE_REPOSITORY_PASSWORD =
            new ConfigOption<>(
                    "k8s.image_repository_password",
                    "The password for login image repository.",
                    null,
                    ""
            );

    public static final ConfigOption<String> IMAGE_REPOSITORY_URL =
            new ConfigOption<>(
                    "k8s.image_repository_url",
                    "The url of image repository.",
                    disallowEmpty(),
                    "hugegraph/hugegraph-computer"
            );

    public static final ConfigOption<String> JAR_FILE_DIR =
            new ConfigOption<>(
                    "k8s.jar_file_dir",
                    "The directory where the algorithm jar to upload location.",
                    disallowEmpty(),
                    "/cache/jars/"
            );

    public static final ConfigListOption<String> PULL_SECRET_NAMES =
            new ConfigListOption<>(
                    "k8s.pull_secret_names",
                    "The names of pull-secret for pulling image.",
                    null,
                    ""
            );

    public static final ConfigOption<String> LOG4J_XML_PATH =
            new ConfigOption<>(
                    "k8s.log4j_xml_path",
                    "The log4j.xml path for computer job.",
                    null,
                    ""
            );

    public static final ConfigOption<Boolean> ENABLE_INTERNAL_ALGORITHM =
            new ConfigOption<>(
                    "k8s.enable_internal_algorithm",
                    "Whether enable internal algorithm.",
                    allowValues(true, false),
                    true
            );

    public static final ConfigListOption<String> INTERNAL_ALGORITHMS =
            new ConfigListOption<>(
                    "k8s.internal_algorithm",
                    "The name list of all internal algorithm.",
                    disallowEmpty(),
                    "pageRank"
            );

    public static final ConfigOption<String> INTERNAL_ALGORITHM_IMAGE_URL =
            new ConfigOption<>(
                    "k8s.internal_algorithm_image_url",
                    "The image url of internal algorithm.",
                    disallowEmpty(),
                    "hugegraph/hugegraph-computer:latest"
            );
}
