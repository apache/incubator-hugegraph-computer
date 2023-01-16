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

package org.apache.hugegraph.computer.core.util;

import java.util.Map;
import java.util.Properties;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.worker.MockComputationParams;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class ComputerContextUtilTest {

    @Test
    public void testConvertToMap() {
        Assert.assertThrows(ComputerException.class, () -> {
            ComputerContextUtil.convertToMap((String) null);
        });

        Assert.assertThrows(ComputerException.class, () -> {
            ComputerContextUtil.convertToMap((Properties) null);
        });

        Assert.assertThrows(ComputerException.class, () -> {
            ComputerContextUtil.convertToMap(new String[0]);
        });

        Assert.assertThrows(ComputerException.class, () -> {
            ComputerContextUtil.convertToMap("just one");
        });

        Map<String, String> result = ComputerContextUtil.convertToMap(
                                     "key1", "value1", "key2", "value2");
        Assert.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"),
                            result);

        Properties properties = new Properties();
        properties.setProperty("key1", "newValue1");
        properties.setProperty("algorithm.params_class",
                               MockComputationParams.class.getName());
        result = ComputerContextUtil.convertToMap(properties);
        Assert.assertEquals(ImmutableMap.of(
                            "key1", "newValue1",
                            "algorithm.params_class",
                            MockComputationParams.class.getName()), result);

        ComputerContextUtil.initContext(properties);
        Assert.assertEquals("newValue1",
                            ComputerContext.instance()
                                           .config()
                                           .getString("key1", ""));
    }
}
