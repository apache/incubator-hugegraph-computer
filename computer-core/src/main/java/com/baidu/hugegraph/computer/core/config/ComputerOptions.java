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
                    "computer.algorithm.name",
                    "The name of current algorithm",
                    disallowEmpty(),
                    "page_rank"
            );

    public static final ConfigOption<String> VALUE_TYPE =
            new ConfigOption<>(
                    "computer.value.type",
                    "The value type of current algorithm",
                    disallowEmpty(),
                    "NULL"
            );

    public static final ConfigOption<String> VERTEX_VALUE_NAME =
            new ConfigOption<>(
                    "computer.vertex.value.name",
                    "The output value name of vertex",
                    disallowEmpty(),
                    "rank"
            );

    public static final ConfigOption<String> EDGE_VALUE_NAME =
            new ConfigOption<>(
                    "computer.edge.value.name",
                    "The output value name of edge",
                    disallowEmpty(),
                    "value"
            );

    public static final ConfigOption<Boolean> OUTPUT_VERTEX_OUT_EDGES =
            new ConfigOption<>(
                    "computer.output.vertex.out_edges",
                    "Output the edges of the vertex or not",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Boolean> OUTPUT_VERTEX_PROPERTIES =
            new ConfigOption<>(
                    "computer.output.vertex.properties",
                    "Output the properties of the vertex or not",
                    allowValues(true, false),
                    false
            );

    public static final ConfigOption<Boolean> OUTPUT_EDGE_PROPERTIES =
            new ConfigOption<>(
                    "computer.output.edge.properties",
                    "Output the properties of the edge or not",
                    allowValues(true, false),
                    false
            );

    public static Set<String> REQUIRED_OPTIONS = ImmutableSet.of(
            ALGORITHM_NAME.name(),
            VALUE_TYPE.name(),
            VERTEX_VALUE_NAME.name(),
            EDGE_VALUE_NAME.name()
    );
}
