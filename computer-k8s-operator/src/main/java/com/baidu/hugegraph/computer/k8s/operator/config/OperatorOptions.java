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

package com.baidu.hugegraph.computer.k8s.operator.config;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.positiveInt;

import com.baidu.hugegraph.computer.k8s.Constants;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;

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
                    "WATCH_NAMESPACE",
                    "The value is watch custom resources in the namespace, " +
                    "ignore other namespaces, the '*' means is all namespaces" +
                    " will be watched.",
                    disallowEmpty(),
                    Constants.DEFAULT_NAMESPACE
            );

    public static final ConfigOption<Integer> PROBE_PORT =
            new ConfigOption<>(
                    "PROBE_PORT",
                    "The value is the port that the controller bind to for " +
                    "serving health probes.",
                    positiveInt(),
                    8081
            );

    public static final ConfigOption<Integer> PROBE_BACKLOG =
            new ConfigOption<>(
                    "PROBE_BACKLOG",
                    "The maximum backlog for serving health probes.",
                    positiveInt(),
                    50
            );

    public static final ConfigOption<Long> RESYNC_PERIOD =
            new ConfigOption<>(
                    "RESYNC_PERIOD",
                    "The minimum frequency at which watched resources are " +
                    "reconciled.",
                    positiveInt(),
                    10 * 1000L
            );

    public static final ConfigOption<Integer> WORKER_COUNT =
            new ConfigOption<>(
                    "WORKER_COUNT",
                    "The max number of reconciler thread.",
                    positiveInt(),
                    Runtime.getRuntime().availableProcessors()
            );

    public static final ConfigOption<Long> READY_CHECK_INTERNAL =
            new ConfigOption<>(
                    "READY_CHECK_INTERNAL",
                    "The time interval(ms) of check ready.",
                    positiveInt(),
                    1000L
            );

    public static final ConfigOption<Long> READY_TIMEOUT =
            new ConfigOption<>(
                    "READY_TIMEOUT",
                    "The max timeout(in ms) of check ready.",
                    positiveInt(),
                    30 * 1000L
            );

    public static final ConfigOption<Integer> MAX_RECONCILE_RETRY =
            new ConfigOption<>(
                    "MAX_RECONCILE_RETRY",
                    "The max retry times of reconcile.",
                    positiveInt(),
                    3
            );
}
