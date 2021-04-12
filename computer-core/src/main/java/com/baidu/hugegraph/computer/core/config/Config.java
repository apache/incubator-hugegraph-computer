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

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.TypedOption;
import com.baidu.hugegraph.util.E;

public final class Config {

    private final HugeConfig allConfig;
    private final HotConfig hotConfig;

    public Config(String... options) {
        this(convertToMap(options));
    }

    public Config(Map<String, String> options) {
        this.allConfig = this.parseOptions(options);
        this.hotConfig = this.extractHotConfig(this.allConfig);
    }

    private HugeConfig parseOptions(Map<String, String> options) {
        if (!options.keySet().containsAll(ComputerOptions.REQUIRED_OPTIONS)) {
            E.checkArgument(false,
                            "All required options must be setted, " +
                            "but missing these %s",
                            CollectionUtils.subtract(
                            ComputerOptions.REQUIRED_OPTIONS,
                            options.keySet()));
        }
        MapConfiguration mapConfig = new MapConfiguration(options);
        return new HugeConfig(mapConfig);
    }

    private HotConfig extractHotConfig(HugeConfig allConfig) {
        // Populate high frequency accessed options into HotConfig
        HotConfig hotConfig = new HotConfig();
        hotConfig.algorithmName(allConfig.get(ComputerOptions.ALGORITHM_NAME));
        hotConfig.vertexValueName(
                  allConfig.get(ComputerOptions.VALUE_NAME));
        hotConfig.edgeValueName(
                  allConfig.get(ComputerOptions.EDGES_NAME));
        hotConfig.valueType(ValueType.valueOf(
                  allConfig.get(ComputerOptions.VALUE_TYPE)));

        hotConfig.outputVertexAdjacentEdges(
                  allConfig.get(ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES));
        hotConfig.outputVertexProperties(
                  allConfig.get(ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES));
        hotConfig.outputEdgeProperties(
                  allConfig.get(ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES));
        return hotConfig;
    }

    public <R> R get(TypedOption<?, R> option) {
        return this.allConfig.get(option);
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = this.allConfig.getString(key);
        if (value == null) {
            return defaultValue;
        } else if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        } else {
            throw new ComputerException(
                      "Can't parse boolean value from '%s' for key '%s'",
                      value, key);
        }
    }

    public int getInt(String key, int defaultValue) {
        String value = this.allConfig.getString(key);
        if (value == null) {
            return defaultValue;
        } else {
            try {
                return Integer.parseInt(value);
            } catch (Exception e) {
                throw new ComputerException(
                          "Can't parse int value from '%s' for key '%s'",
                          value, key);
            }
        }
    }

    public long getLong(String key, long defaultValue) {
        String value = this.allConfig.getString(key);
        if (value == null) {
            return defaultValue;
        } else {
            try {
                return Long.parseLong(value);
            } catch (Exception e) {
                throw new ComputerException(
                          "Can't parse long value from '%s' for key '%s'",
                          value, key);
            }
        }
    }

    public double getDouble(String key, double defaultValue) {
        String value = this.allConfig.getString(key);
        if (value == null) {
            return defaultValue;
        } else {
            try {
                return Double.parseDouble(value);
            } catch (Exception e) {
                throw new ComputerException(
                          "Can't parse double value from '%s' for key '%s'",
                          value, key);
            }
        }
    }

    public String getString(String key, String defaultValue) {
        return this.allConfig.getString(key, defaultValue);
    }

    /**
     * Create object by class option. It throws ComputerException if failed
     * to create object.
     */
    public <T> T createObject(ConfigOption<Class<?>> clazzOption) {
        Class<?> clazz = this.get(clazzOption);
        if (clazz == Null.class) {
            return null;
        }
        try {
            @SuppressWarnings("unchecked")
            T instance = (T) clazz.newInstance();
            return instance;
        } catch (Exception e) {
            throw new ComputerException("Failed to create object for option " +
                                        "'%s', class='%s'",
                                        e, clazzOption.name(), clazz.getName());
        }
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

    public Boolean outputVertexAdjacentEdges() {
        return this.hotConfig.outputVertexAdjacentEdges();
    }

    public Boolean outputVertexProperties() {
        return this.hotConfig.outputVertexProperties();
    }

    public Boolean outputEdgeProperties() {
        return this.hotConfig.outputEdgeProperties();
    }

    private static Map<String, String> convertToMap(String... options) {
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
        return map;
    }
}
