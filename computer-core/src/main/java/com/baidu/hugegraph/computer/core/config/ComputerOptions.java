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

package com.baidu.hugegraph.computer.core.config;

import static com.baidu.hugegraph.config.OptionChecker.allowValues;
import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.positiveInt;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;
import com.google.common.collect.ImmutableSet;

public class ComputerOptions extends OptionHolder {

    private ComputerOptions() {
        super();
    }

    private static volatile ComputerOptions INSTANCE;

    public static synchronized ComputerOptions instance() {
        if (INSTANCE == null) {
            INSTANCE = new ComputerOptions();
            // Should initialize all static members first, then register.
            INSTANCE.registerOptions();
        }
        return INSTANCE;
    }

    public static final ConfigOption<String> ALGORITHM_NAME =
            new ConfigOption<>(
                    "algorithm.name",
                    "The name of current algorithm",
                    disallowEmpty(),
                    "unknown"
            );

    public static final ConfigOption<String> VALUE_TYPE =
            new ConfigOption<>(
                    "algorithm.value_type",
                    "The value type of current algorithm",
                    disallowEmpty(),
                    "NULL"
            );

    public static final ConfigOption<String> VALUE_NAME =
            new ConfigOption<>(
                    "algorithm.value_name",
                    "The algorithm value name of vertex",
                    disallowEmpty(),
                    "value"
            );

    public static final ConfigOption<String> EDGES_NAME =
            new ConfigOption<>(
                    "algorithm.edges_name",
                    "The algorithm value name of edges",
                    disallowEmpty(),
                    "value"
            );

    public static final ConfigOption<Long> INPUT_SPLITS_SIZE =
            new ConfigOption<>(
                    "input.split_size",
                    "The input split size in bytes",
                    positiveInt(),
                    1024 * 1024L
            );

    public static final ConfigOption<Integer> INPUT_SPLITS_MAX_NUMBER =
            new ConfigOption<>(
                    "input.split_max_number",
                    "The maximum number of input splits",
                    positiveInt(),
                    10_000_000
            );

    public static final ConfigOption<Integer> INPUT_SPLIT_PAGE_SIZE =
            new ConfigOption<>(
                    "input.split_page_size",
                    "The page size for streamed load input split data",
                    positiveInt(),
                    500
            );

    public static final ConfigOption<Boolean> OUTPUT_WITH_ADJACENT_EDGES =
            new ConfigOption<>(
                    "output.with_adjacent_edges",
                    "Output the adjacent edges of the vertex or not",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Boolean> OUTPUT_WITH_VERTEX_PROPERTIES =
            new ConfigOption<>(
                    "output.with_vertex_properties",
                    "Output the properties of the vertex or not",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Boolean> OUTPUT_WITH_EDGE_PROPERTIES =
            new ConfigOption<>(
                    "output.with_edge_properties",
                    "Output the properties of the edge or not",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Integer> VERTEX_AVERAGE_DEGREE =
            new ConfigOption<>(
                    "computer.vertex_average_degree",
                    "The average degree of a vertex, it represents the " +
                    "average number of adjacent edges per vertex",
                    positiveInt(),
                    10
            );

    public static final ConfigOption<Integer>
           ALLOCATOR_MAX_VERTICES_PER_THREAD =
            new ConfigOption<>(
                    "allocator.max_vertices_per_thread",
                    "Maximum number of vertices per thread processed " +
                    "in each memory allocator",
                    positiveInt(),
                    10000
            );

    public static Set<String> REQUIRED_OPTIONS = ImmutableSet.of(
            ALGORITHM_NAME.name(),
            VALUE_TYPE.name(),
            VALUE_NAME.name(),
            EDGES_NAME.name()
    );

    public static final ConfigOption<String> JOB_ID =
            new ConfigOption<>(
                    "job.id",
                    "The job id on Yarn cluster or K8s cluster.",
                    disallowEmpty(),
                    "local_0001"
            );

    public static final ConfigOption<Integer> JOB_WORKERS_COUNT =
            new ConfigOption<>(
                    "job.workers_count",
                    "The workers count for computing one graph " +
                    "algorithm job.",
                    positiveInt(),
                    1
            );

    public static final ConfigOption<Integer> BSP_MAX_SUPER_STEP =
            new ConfigOption<>(
                    "bsp.max_super_step",
                    "The max super step of the algorithm.",
                    positiveInt(),
                    10
            );

    public static final ConfigOption<String> BSP_ETCD_ENDPOINTS =
            new ConfigOption<>(
                    "bsp.etcd_endpoints",
                    "The end points to access etcd.",
                    disallowEmpty(),
                    "http://localhost:2379"
            );

    public static final ConfigOption<Long> BSP_REGISTER_TIMEOUT =
            new ConfigOption<>(
                    "bsp.register_timeout",
                    "The max timeout to wait for master and works to register.",
                    positiveInt(),
                    TimeUnit.MINUTES.toMillis(5L)
            );

    public static final ConfigOption<Long> BSP_WAIT_WORKERS_TIMEOUT =
            new ConfigOption<>(
                    "bsp.wait_workers_timeout",
                    "The max timeout to wait for workers to sent bsp event.",
                    positiveInt(),
                    TimeUnit.HOURS.toMillis(24L)
            );

    public static final ConfigOption<Long> BSP_WAIT_MASTER_TIMEOUT =
            new ConfigOption<>(
                    "bsp.wait_master_timeout",
                    "The max timeout(in ms) to wait for master to sent bsp " +
                    "event.",
                    positiveInt(),
                    TimeUnit.HOURS.toMillis(24L)
            );

    public static final ConfigOption<Long> BSP_LOG_INTERVAL =
            new ConfigOption<>(
                    "bsp.log_interval",
                    "The log interval(in ms) to print the log while " +
                    "waiting bsp event.",
                    positiveInt(),
                    TimeUnit.SECONDS.toMillis(30L)
            );
}
