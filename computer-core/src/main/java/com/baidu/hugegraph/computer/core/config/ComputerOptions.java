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

    public static final ConfigOption<Boolean> OUTPUT_WITH_ADJACENT_EDGES =
            new ConfigOption<>(
                    "computer.output.with_adjacent_edges",
                    "Output the adjacent edges of the vertex or not",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Boolean> OUTPUT_WITH_VERTEX_PROPERTIES =
            new ConfigOption<>(
                    "computer.output.with_vertex_properties",
                    "Output the properties of the vertex or not",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Boolean> OUTPUT_WITH_EDGE_PROPERTIES =
            new ConfigOption<>(
                    "computer.output.with_edge_properties",
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
                    "in parallel",
                    positiveInt(),
                    10000
            );

    public static Set<String> REQUIRED_OPTIONS = ImmutableSet.of(
            ALGORITHM_NAME.name(),
            VALUE_TYPE.name(),
            VALUE_NAME.name(),
            EDGES_NAME.name()
    );
}
