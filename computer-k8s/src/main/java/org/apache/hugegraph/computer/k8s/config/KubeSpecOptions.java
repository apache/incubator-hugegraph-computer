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
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.computer.driver.config.DriverConfigOption;
import org.apache.hugegraph.computer.k8s.Constants;
import org.apache.hugegraph.config.ConfigListOption;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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

    public static final DriverConfigOption<String> MASTER_CPU =
            new DriverConfigOption<>(
                    "k8s.master_cpu",
                    "The cpu limit of master, the unit can be 'm' or without " +
                    "unit detail please refer to：https://kubernetes " +
                    ".io/zh/docs/concepts/configuration/" +
                    "manage-resources-containers/#meaning-of-cpu",
                    KubeSpecOptions::checkQuantity,
                    String.class
            );

    public static final DriverConfigOption<String> MASTER_MEMORY =
            new DriverConfigOption<>(
                    "k8s.master_memory",
                    "The memory limit of master, the unit can be " +
                    "one of Ei、Pi、Ti、Gi、Mi、Ki " +
                    "detail please refer to：https://kubernetes " +
                    ".io/zh/docs/concepts/configuration/" +
                    "manage-resources-containers/#meaning-of-memory",
                    KubeSpecOptions::checkQuantity,
                    String.class
            );

    public static final ConfigListOption<String> MASTER_COMMAND =
            new ConfigListOption<>(
                    "k8s.master_command",
                    "The run command of master, equivalent to " +
                    "'Entrypoint' field of Docker.",
                    disallowEmpty(),
                    Constants.COMMAND.toArray(new String[0])
            );

    public static final ConfigListOption<String> MASTER_ARGS =
            new ConfigListOption<>(
                    "k8s.master_args",
                    "The run args of master, equivalent to 'Cmd' " +
                    "field of Docker.",
                    disallowEmpty(),
                    Constants.MASTER_ARGS.toArray(new String[0])
            );

    public static final DriverConfigOption<String> WORKER_CPU =
            new DriverConfigOption<>(
                    "k8s.worker_cpu",
                    "The cpu limit of worker, the unit can be 'm' or without " +
                    "unit detail please refer to：https://kubernetes " +
                    ".io/zh/docs/concepts/configuration/" +
                    "manage-resources-containers/#meaning-of-cpu",
                    KubeSpecOptions::checkQuantity,
                    String.class
            );

    public static final DriverConfigOption<String> WORKER_MEMORY =
            new DriverConfigOption<>(
                    "k8s.worker_memory",
                    "The memory limit of worker, the unit can be " +
                    "one of Ei、Pi、Ti、Gi、Mi、Ki " +
                    "detail please refer to：https://kubernetes " +
                    ".io/zh/docs/concepts/configuration/" +
                    "manage-resources-containers/#meaning-of-memory",
                    KubeSpecOptions::checkQuantity,
                    String.class
            );

    public static final ConfigListOption<String> WORKER_COMMAND =
            new ConfigListOption<>(
                    "k8s.worker_command",
                    "The run command of worker, equivalent to " +
                    "'Entrypoint' field of Docker.",
                    disallowEmpty(),
                    Constants.COMMAND.toArray(new String[0])
            );

    public static final ConfigListOption<String> WORKER_ARGS =
            new ConfigListOption<>(
                    "k8s.worker_args",
                    "The run args of worker, equivalent to 'Cmd' " +
                    "field of Docker.",
                    disallowEmpty(),
                    Constants.WORKER_ARGS.toArray(new String[0])
            );

    public static final ConfigOption<Integer> WORKER_INSTANCES =
            new ConfigOption<>(
                    "k8s.worker_instances",
                    "The number of worker instances, it will " +
                    "instead the 'job.workers_count' option.",
                    rangeInt(1, Integer.MAX_VALUE),
                    1
            );

    public static final ConfigOption<String> PULL_POLICY =
            new ConfigOption<>(
                    "k8s.pull_policy",
                    "The pull-policy of image.",
                    allowValues("Always", "Never", "IfNotPresent"),
                    "Always"
            );

    public static final DriverConfigOption<String> JVM_OPTIONS =
            new DriverConfigOption<>(
                    "k8s.jvm_options",
                    "The java startup parameters of computer job.",
                    disallowEmpty(),
                    String.class
            );

    public static final DriverConfigOption<String> REMOTE_JAR_URI =
            new DriverConfigOption<>(
                    "k8s.remote_jar_uri",
                    "The remote jar uri of computer job, it will overlay " +
                    "algorithm image.",
                    disallowEmpty(),
                    String.class
            );

    public static final ConfigListOption<String> CONFIG_MAP_PATHS =
            new ConfigListOption<>(
                    "k8s.config_map_paths",
                    "The map of k8s-configmap' name and mount path.",
                    null,
                    ""
            );

    public static final ConfigListOption<String> SECRET_PATHS =
            new ConfigListOption<>(
                    "k8s.secret_paths",
                    "The map of k8s-secret' name and mount path.",
                    null,
                    ""
            );

    public static final Map<String, ConfigOption<?>> ALLOW_USER_SETTINGS =
            new ImmutableMap.Builder<String, ConfigOption<?>>()
                    .put(MASTER_CPU.name(), MASTER_CPU)
                    .put(WORKER_CPU.name(), WORKER_CPU)
                    .put(MASTER_MEMORY.name(), MASTER_MEMORY)
                    .put(WORKER_MEMORY.name(), WORKER_MEMORY)
                    .put(WORKER_INSTANCES.name(), WORKER_INSTANCES)
                    .put(JVM_OPTIONS.name(), JVM_OPTIONS)
                    .put(REMOTE_JAR_URI.name(), REMOTE_JAR_URI)
                    .put(CONFIG_MAP_PATHS.name(), CONFIG_MAP_PATHS)
                    .put(SECRET_PATHS.name(), SECRET_PATHS)
                    .build();

    public static final Set<ConfigOption<?>> MAP_TYPE_CONFIGS = ImmutableSet.of(
            CONFIG_MAP_PATHS,
            SECRET_PATHS
    );
}
