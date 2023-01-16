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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.computer.core.common.FakeMasterComputation;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.master.DefaultMasterComputation;
import org.apache.hugegraph.computer.core.master.MasterComputation;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class DefaultConfigTest extends UnitTestBase {

    private static final String KEY = "algorithm.page_rank.key";
    private static final String KEY_TRUE = "algorithm.page_rank.key_true";
    private static final String KEY_FALSE = "algorithm.page_rank.key_false";
    private static final String KEY_MAX = "algorithm.page_rank.key_max";
    private static final String KEY_MIN = "algorithm.page_rank.key_min";
    private static final String KEY_EMPTY = "algorithm.page_rank.no_key";
    private static final String KEY_ABC = "algorithm.page_rank.abc";
    private static final String VALUE_ABC = "abc";

    @Test
    public void testGetBoolean() {
        final boolean defaultValue = false;
        Map<String, String> options = this.initialOptions();
        options.put(KEY_TRUE, Boolean.toString(Boolean.TRUE));
        options.put(KEY_FALSE, Boolean.toString(Boolean.FALSE));
        Config config = new DefaultConfig(options);
        Assert.assertTrue(config.getBoolean(KEY_TRUE, defaultValue));
        Assert.assertFalse(config.getBoolean(KEY_FALSE, defaultValue));
        Assert.assertFalse(config.getBoolean(KEY_EMPTY, defaultValue));
        Assert.assertThrows(ComputerException.class, () -> {
            config.getBoolean(KEY_ABC, Boolean.TRUE);
        } ,e -> {
            Assert.assertContains("Can't parse boolean value", e.getMessage());
        });
    }

    @Test
    public void testGetInt() {
        final int defaultValue = 1;
        Map<String, String> options = this.initialOptions();
        options.put(KEY_MAX, Integer.toString(Integer.MAX_VALUE));
        options.put(KEY_MIN, Integer.toString(Integer.MIN_VALUE));
        Config config = new DefaultConfig(options);
        Assert.assertEquals(Integer.MAX_VALUE,
                            config.getInt(KEY_MAX, defaultValue));
        Assert.assertEquals(Integer.MIN_VALUE,
                            config.getInt(KEY_MIN, defaultValue));
        Assert.assertEquals(defaultValue,
                            config.getInt(KEY_EMPTY, defaultValue));
        Assert.assertThrows(ComputerException.class, () -> {
            config.getInt(KEY_ABC, defaultValue);
        } ,e -> {
            Assert.assertContains("Can't parse int value", e.getMessage());
        });
    }

    @Test
    public void testGetLong() {
        final long defaultValue = 1L;
        Map<String, String> options = this.initialOptions();
        options.put(KEY_MAX, Long.toString(Long.MAX_VALUE));
        options.put(KEY_MIN, Long.toString(Long.MIN_VALUE));
        Config config = new DefaultConfig(options);
        Assert.assertEquals(Long.MAX_VALUE,
                            config.getLong(KEY_MAX, defaultValue));
        Assert.assertEquals(Long.MIN_VALUE,
                            config.getLong(KEY_MIN, defaultValue));
        Assert.assertEquals(defaultValue,
                            config.getLong(KEY_EMPTY, defaultValue));
        Assert.assertThrows(ComputerException.class, () -> {
            config.getLong(KEY_ABC, defaultValue);
        } ,e -> {
            Assert.assertContains("Can't parse long value", e.getMessage());
        });
    }

    @Test
    public void testGetDouble() throws IOException {
        final double defaultValue = 1.0D;
        final double delta = 0.0D;
        Map<String, String> options = this.initialOptions();
        options.put(KEY_MAX, Double.toString(Double.MAX_VALUE));
        options.put(KEY_MIN, Double.toString(Double.MIN_VALUE));
        Config config = new DefaultConfig(options);
        Assert.assertEquals(Double.MAX_VALUE,
                            config.getDouble(KEY_MAX, defaultValue),
                            delta);
        Assert.assertEquals(Double.MIN_VALUE,
                            config.getDouble(KEY_MIN, defaultValue),
                            delta);
        Assert.assertEquals(defaultValue,
                            config.getDouble(KEY_EMPTY, defaultValue),
                            delta);
        Assert.assertThrows(ComputerException.class, () -> {
            config.getDouble(KEY_ABC, defaultValue);
        } ,e -> {
            Assert.assertContains("Can't parse double value", e.getMessage());
        });
    }

    @Test
    public void testString() throws IOException {
        String value = "The value of string";
        final String defaultValue = "The default value of string";
        Map<String, String> options = this.initialOptions();
        options.put(KEY, value);
        Config config = new DefaultConfig(options);
        Assert.assertEquals(value, config.getString(KEY, defaultValue));
        Assert.assertEquals(value, config.getString(KEY, null));
        Assert.assertEquals(defaultValue,
                            config.getString(KEY_EMPTY, defaultValue));
        Assert.assertNull(config.getString(KEY_EMPTY, null));
    }

    @Test
    public void testCreateObject() {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.MASTER_COMPUTATION_CLASS,
                DefaultMasterComputation.class.getName()
        );
        MasterComputation masterComputation = config.createObject(
                          ComputerOptions.MASTER_COMPUTATION_CLASS);
        Assert.assertEquals(DefaultMasterComputation.class,
                            masterComputation.getClass());
    }

    @Test
    public void testCreateObjectFail() {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.MASTER_COMPUTATION_CLASS,
            FakeMasterComputation.class.getName()
        );
        Assert.assertThrows(ComputerException.class, () -> {
            config.createObject(ComputerOptions.MASTER_COMPUTATION_CLASS);
        }, e -> {
            Assert.assertContains("Failed to create object for option",
                                  e.getMessage());
            Assert.assertContains("with modifiers \"private\"",
                                  e.getCause().getMessage());
        });
    }

    @Test
    public void testNullClass() {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.WORKER_COMBINER_CLASS,
                Null.class.getName()
        );
        Object combiner = config.createObject(
                          ComputerOptions.WORKER_COMBINER_CLASS, false);
        Assert.assertNull(combiner);
    }

    private Map<String, String> initialOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES.name(), "false");
        options.put(ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES.name(),
                    "false");
        options.put(ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES.name(),
                    "false");
        options.put(KEY_ABC, VALUE_ABC);
        return options;
    }
}
