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

package com.baidu.hugegraph.computer.core.common;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.positiveInt;

import java.util.concurrent.TimeUnit;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;

public class ComputerOptions extends OptionHolder {

    private ComputerOptions() {
        super();
    }

    private static volatile ComputerOptions instance;

    public static synchronized ComputerOptions instance() {
        if (instance == null) {
            instance = new ComputerOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> ETCD_ENDPOINTS =
            new ConfigOption<>(
                    "computer.etcd_endpoints",
                    "The end points to access etcd.",
                    disallowEmpty(),
                    "http://localhost:2379"
            );

    public static final ConfigOption<String> JOB_ID =
            new ConfigOption<>(
                    "computer.job_id",
                    "The job id.",
                    disallowEmpty(),
                    "local_0001"
            );

    public static final ConfigOption<Integer> WORKER_COUNT =
            new ConfigOption<>(
                    "computer.worker_count",
                    "The worker count of the algorithm.",
                    positiveInt(),
                    1
            );

    public static final ConfigOption<Integer> MAX_SUPER_STEP =
            new ConfigOption<>(
                    "computer.max_super_step",
                    "The max super step of the algorithm.",
                    positiveInt(),
                    10
            );

    public static final ConfigOption<Long> BSP_REGISTER_TIMEOUT =
            new ConfigOption<>(
                    "computer.bsp_register_timeout",
                    "The max timeout to wait for master and works to register.",
                    positiveInt(),
                    TimeUnit.MINUTES.toMillis(5L)
            );

    public static final ConfigOption<Long> BSP_BARRIER_ON_WORKERS_TIMEOUT =
            new ConfigOption<>(
                    "computer.bsp_barrier_workers_timeout",
                    "The max timeout to wait for workers to sent bsp event.",
                    positiveInt(),
                    TimeUnit.HOURS.toMillis(24L)
            );

    public static final ConfigOption<Long> BSP_BARRIER_ON_MASTER_TIMEOUT =
            new ConfigOption<>(
                    "computer.bsp_barrier_master_timeout",
                    "The max timeout(in ms) to wait for master to sent bsp " +
                    "event.",
                    positiveInt(),
                    TimeUnit.HOURS.toMillis(24L)
            );

    public static final ConfigOption<Long> BSP_LOG_INTERVAL =
            new ConfigOption<>(
                    "computer.bsp_log_interval",
                    "The max timeout to wait master to sent bsp event.",
                    positiveInt(),
                    TimeUnit.SECONDS.toMillis(30L)
            );
}
