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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.MapConfiguration;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.TypedOption;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public final class Config {

    private static final Logger LOG = Log.logger(Config.class);

    private static Config INSTANCE = null;

    private Config(HugeConfig allConfig, HotConfig hotConfig) {
        this.allConfig = allConfig;
        this.hotConfig = hotConfig;
    }

    public static Config instance() {
        if (INSTANCE == null) {
            throw new ComputerException("Should call parseOptions() first");
        }
        return INSTANCE;
    }

    private final HugeConfig allConfig;
    private final HotConfig hotConfig;

    public synchronized static Config parseOptions(String... options) {
        if (options == null || options.length == 0) {
            throw new ComputerException("Config options can't be null " +
                                        "or empty");
        }
        if ((options.length & 0x01) == 1) {
            throw new ComputerException("Config options length must be even");
        }
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < options.length; i += 2) {
            map.put(options[i], options[i + 1]);
        }
        return parseOptions(map);
    }

    public synchronized static Config parseOptions(
                                      Map<String, String> options) {
        if (!options.keySet().containsAll(ComputerOptions.REQUIRED_OPTIONS)) {
            E.checkArgument(false,
                            "All required options must be setted, " +
                            "but missing these %s",
                            CollectionUtils.subtract(
                            ComputerOptions.REQUIRED_OPTIONS,
                            options.keySet()));
        }
        MapConfiguration mapConfig = new MapConfiguration(options);
        HugeConfig allConfig = new HugeConfig(mapConfig);

        // Populate high frequency accessed options into HotConfig
        HotConfig hotConfig = new HotConfig();
        hotConfig.algorithmName(allConfig.get(ComputerOptions.ALGORITHM_NAME));
        hotConfig.vertexValueName(
                  allConfig.get(ComputerOptions.VERTEX_VALUE_NAME));
        hotConfig.edgeValueName(
                  allConfig.get(ComputerOptions.EDGE_VALUE_NAME));
        hotConfig.valueType(ValueType.valueOf(
                            allConfig.get(ComputerOptions.VALUE_TYPE)));

        hotConfig.outputVertexOutEdges(
                  allConfig.get(ComputerOptions.OUTPUT_VERTEX_OUT_EDGES));
        hotConfig.outputVertexProperties(
                  allConfig.get(ComputerOptions.OUTPUT_VERTEX_PROPERTIES));
        hotConfig.outputEdgeProperties(
                  allConfig.get(ComputerOptions.OUTPUT_EDGE_PROPERTIES));
        INSTANCE = new Config(allConfig, hotConfig);
        return INSTANCE;
    }

    public <R> R get(TypedOption<?, R> option) {
        return this.allConfig.get(option);
    }

    public String algorithmName() {
        return this.hotConfig.algorithmName();
    }

    public String vertexValueName() {
        return this.hotConfig.vertexValueName();
    }

    public String edgeValueName() {
        return this.hotConfig.edgeValueName();
    }

    public ValueType valueType() {
        return this.hotConfig.valueType();
    }

    public Boolean outputVertexOutEdges() {
        return this.hotConfig.outputVertexOutEdges();
    }

    public Boolean outputVertexProperties() {
        return this.hotConfig.outputVertexProperties();
    }

    public Boolean outputEdgeProperties() {
        return this.hotConfig.outputEdgeProperties();
    }
}
