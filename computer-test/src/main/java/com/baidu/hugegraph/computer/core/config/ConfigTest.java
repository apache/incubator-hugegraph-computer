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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ConfigTest {

    private static final String KEY = "algorithm.page_rank.key";
    private static final String KEY_MAX = "algorithm.page_rank.key_max";
    private static final String KEY_MIN = "algorithm.page_rank.key_min";
    private static final String KEY_EMPTY = "algorithm.page_rank.no_key";

    @Test
    public void testGetByte() throws IOException {
        final byte defaultValue = (byte) 1;
        Map<String, String> options = this.initialOptions();
        options.put(KEY_MAX, Byte.toString(Byte.MAX_VALUE));
        options.put(KEY_MIN, Byte.toString(Byte.MIN_VALUE));
        Config config = new Config(options);
        Assert.assertEquals(Byte.MAX_VALUE,
                            config.getByte(KEY_MAX, defaultValue));
        Assert.assertEquals(Byte.MIN_VALUE,
                            config.getByte(KEY_MIN, defaultValue));
        Assert.assertEquals(defaultValue,
                            config.getByte(KEY_EMPTY, defaultValue));
    }

    @Test
    public void testGetShort() throws IOException {
        final short defaultValue = (short) 1;
        Map<String, String> options = this.initialOptions();
        options.put(KEY_MAX, Short.toString(Short.MAX_VALUE));
        options.put(KEY_MIN, Short.toString(Short.MIN_VALUE));
        Config config = new Config(options);
        Assert.assertEquals(Short.MAX_VALUE,
                            config.getShort(KEY_MAX, defaultValue));
        Assert.assertEquals(Short.MIN_VALUE,
                            config.getShort(KEY_MIN, defaultValue));
        Assert.assertEquals(defaultValue,
                            config.getShort(KEY_EMPTY, defaultValue));
    }

    @Test
    public void testGetInt() throws IOException {
        final int defaultValue = 1;
        Map<String, String> options = this.initialOptions();
        options.put(KEY_MAX, Integer.toString(Integer.MAX_VALUE));
        options.put(KEY_MIN, Integer.toString(Integer.MIN_VALUE));
        Config config = new Config(options);
        Assert.assertEquals(Integer.MAX_VALUE,
                            config.getInt(KEY_MAX, defaultValue));
        Assert.assertEquals(Integer.MIN_VALUE,
                            config.getInt(KEY_MIN, defaultValue));
        Assert.assertEquals(defaultValue,
                            config.getInt(KEY_EMPTY, defaultValue));
    }

    @Test
    public void testGetLong() throws IOException {
        final long defaultValue = 1L;
        Map<String, String> options = this.initialOptions();
        options.put(KEY_MAX, Long.toString(Long.MAX_VALUE));
        options.put(KEY_MIN, Long.toString(Long.MIN_VALUE));
        Config config = new Config(options);
        Assert.assertEquals(Long.MAX_VALUE,
                            config.getLong(KEY_MAX, defaultValue));
        Assert.assertEquals(Long.MIN_VALUE,
                            config.getLong(KEY_MIN, defaultValue));
        Assert.assertEquals(defaultValue,
                            config.getLong(KEY_EMPTY, defaultValue));
    }

    @Test
    public void testGetFloat() throws IOException {
        final float defaultValue = 1.0F;
        final float delta = 0.0F;
        Map<String, String> options = this.initialOptions();
        options.put(KEY_MAX, Float.toString(Float.MAX_VALUE));
        options.put(KEY_MIN, Float.toString(Float.MIN_VALUE));
        Config config = new Config(options);
        Assert.assertEquals(Float.MAX_VALUE,
                            config.getFloat(KEY_MAX, defaultValue),
                            delta);
        Assert.assertEquals(Float.MIN_VALUE,
                            config.getFloat(KEY_MIN, defaultValue),
                            delta);
        Assert.assertEquals(defaultValue,
                            config.getFloat(KEY_EMPTY, defaultValue),
                            delta);
    }

    @Test
    public void testGetDouble() throws IOException {
        final double defaultValue = 1.0D;
        final double delta = 0.0D;
        Map<String, String> options = this.initialOptions();
        options.put(KEY_MAX, Double.toString(Double.MAX_VALUE));
        options.put(KEY_MIN, Double.toString(Double.MIN_VALUE));
        Config config = new Config(options);
        Assert.assertEquals(Double.MAX_VALUE,
                            config.getDouble(KEY_MAX, defaultValue),
                            delta);
        Assert.assertEquals(Double.MIN_VALUE,
                            config.getDouble(KEY_MIN, defaultValue),
                            delta);
        Assert.assertEquals(defaultValue,
                            config.getDouble(KEY_EMPTY, defaultValue),
                            delta);
    }

    @Test
    public void testString() throws IOException {

        String value = "The value of string";
        final String defaultValue = "The default value of string";
        Map<String, String> options = this.initialOptions();
        options.put(KEY, value);
        Config config = new Config(options);
        Assert.assertEquals(value, config.getString(KEY, defaultValue));
        Assert.assertEquals(defaultValue,
                            config.getString(KEY_EMPTY, defaultValue));
    }

    private Map<String, String> initialOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(ComputerOptions.ALGORITHM_NAME.name(), "page_rank");
        options.put(ComputerOptions.VALUE_NAME.name(), "rank");
        options.put(ComputerOptions.EDGES_NAME.name(), "value");
        options.put(ComputerOptions.VALUE_TYPE.name(), "LONG");
        options.put(ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES.name(), "false");
        options.put(ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES.name(),
                    "false");
        options.put(ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES.name(),
                    "false");
        return options;
    }
}
