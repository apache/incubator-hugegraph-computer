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

package org.apache.hugegraph.computer.k8s.operator.config;

import static org.apache.hugegraph.config.OptionChecker.allowValues;
import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.positiveInt;

import org.apache.hugegraph.computer.k8s.Constants;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

public class OperatorOptions extends OptionHolder {

    private OperatorOptions() {
        super();
    }

    private static volatile OperatorOptions INSTANCE;

    public static synchronized OperatorOptions instance() {
        if (INSTANCE == null) {
            INSTANCE = new OperatorOptions();
            // Should initialize all static members first, then register.
            INSTANCE.registerOptions();
        }
        return INSTANCE;
    }

    public static final ConfigOption<String> WATCH_NAMESPACE =
            new ConfigOption<>(
                    "k8s.watch_namespace",
                    "The value is watch custom resources in the namespace, " +
                    "ignore other namespaces, the '*' means is all namespaces" +
                    " will be watched.",
                    disallowEmpty(),
                    Constants.DEFAULT_NAMESPACE
            );

    public static final ConfigOption<String> TIMEZONE =
            new ConfigOption<>(
                    "k8s.timezone",
                    "The timezone of computer job and operator.",
                    disallowEmpty(),
                    "Asia/Shanghai"
            );

    public static final ConfigOption<Integer> PROBE_PORT =
            new ConfigOption<>(
                    "k8s.probe_port",
                    "The value is the port that the controller bind to for " +
                    "serving health probes.",
                    positiveInt(),
                    9892
            );

    public static final ConfigOption<Integer> PROBE_BACKLOG =
            new ConfigOption<>(
                    "k8s.probe_backlog",
                    "The maximum backlog for serving health probes.",
                    positiveInt(),
                    50
            );

    public static final ConfigOption<Long> RESYNC_PERIOD =
            new ConfigOption<>(
                    "k8s.resync_period",
                    "The minimum frequency at which watched resources are " +
                    "reconciled.",
                    positiveInt(),
                    10 * 60 * 1000L
            );

    public static final ConfigOption<Integer> RECONCILER_COUNT =
            new ConfigOption<>(
                    "k8s.reconciler_count",
                    "The max number of reconciler thread.",
                    positiveInt(),
                    Runtime.getRuntime().availableProcessors()
            );

    public static final ConfigOption<Long> CLOSE_RECONCILER_TIMEOUT =
            new ConfigOption<>(
                    "k8s.close_reconciler_timeout",
                    "The max timeout(in ms) to close reconciler.",
                    positiveInt(),
                    120L
            );

    public static final ConfigOption<Long> READY_CHECK_INTERNAL =
            new ConfigOption<>(
                    "k8s.ready_check_internal",
                    "The time interval(ms) of check ready.",
                    positiveInt(),
                    1000L
            );

    public static final ConfigOption<Long> READY_TIMEOUT =
            new ConfigOption<>(
                    "k8s.ready_timeout",
                    "The max timeout(in ms) of check ready.",
                    positiveInt(),
                    30 * 1000L
            );

    public static final ConfigOption<Integer> MAX_RECONCILE_RETRY =
            new ConfigOption<>(
                    "k8s.max_reconcile_retry",
                    "The max retry times of reconcile.",
                    positiveInt(),
                    3
            );

    public static final ConfigOption<String> INTERNAL_ETCD_URL =
            new ConfigOption<>(
                    "k8s.internal_etcd_url",
                    "The internal etcd url for operator system.",
                    disallowEmpty(),
                    "http://127.0.0.1:2379"
            );

    public static final ConfigOption<String> INTERNAL_MINIO_URL =
            new ConfigOption<>(
                    "k8s.internal_minio_url",
                    "The internal minio url for operator system.",
                    disallowEmpty(),
                    "http://127.0.0.1:9000"
            );

    public static final ConfigOption<Boolean> AUTO_DESTROY_POD =
            new ConfigOption<>(
                    "k8s.auto_destroy_pod",
                    "Whether to automatically destroy all pods when the job " +
                    "is completed or failed.",
                    allowValues(true, false),
                    true
            );
}
