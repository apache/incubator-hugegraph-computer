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

package org.apache.hugegraph.computer.core.config;

import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.TypedOption;
import org.apache.hugegraph.util.E;

public final class DefaultConfig implements Config {

    private final HugeConfig allConfig;
    private final HotConfig hotConfig;

    public DefaultConfig(Map<String, String> options) {
        this.allConfig = this.parseOptions(options);
        this.hotConfig = this.extractHotConfig(this.allConfig);
        this.checkOptions();
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

        hotConfig.outputVertexAdjacentEdges(
                  allConfig.get(ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES));
        hotConfig.outputVertexProperties(
                  allConfig.get(ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES));
        hotConfig.outputEdgeProperties(
                  allConfig.get(ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES));
        return hotConfig;
    }

    private void checkOptions() {
        int partitionsCount = this.allConfig.get(
                              ComputerOptions.JOB_PARTITIONS_COUNT);
        int workersCount = this.allConfig.get(
                           ComputerOptions.JOB_WORKERS_COUNT);
        if (partitionsCount < workersCount) {
            throw new ComputerException("The partitions count must be >= " +
                                        "workers count, but got %s < %s",
                                        partitionsCount, workersCount);
        }
    }

    @Override
    public HugeConfig hugeConfig() {
        return this.allConfig;
    }

    @Override
    public <R> R get(TypedOption<?, R> option) {
        return this.allConfig.get(option);
    }

    @Override
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

    @Override
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

    @Override
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

    @Override
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

    @Override
    public String getString(String key, String defaultValue) {
        return this.allConfig.getString(key, defaultValue);
    }

    /**
     * Create object by class option.
     * It throws ComputerException if failed to create object.
     */
    @Override
    public <T> T createObject(ConfigOption<Class<?>> clazzOption) {
        return this.createObject(clazzOption, true);
    }

    @Override
    public <T> T createObject(ConfigOption<Class<?>> clazzOption,
                              boolean requiredNotNull) {
        Class<?> clazz = this.get(clazzOption);
        if (clazz == Null.class) {
            if (requiredNotNull) {
                throw new ComputerException(
                      "Please config required option '%s'", clazzOption.name());
            }
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

    @Override
    public Boolean outputVertexAdjacentEdges() {
        return this.hotConfig.outputVertexAdjacentEdges();
    }

    @Override
    public Boolean outputVertexProperties() {
        return this.hotConfig.outputVertexProperties();
    }

    @Override
    public Boolean outputEdgeProperties() {
        return this.hotConfig.outputEdgeProperties();
    }
}
