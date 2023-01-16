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

package org.apache.hugegraph.computer.driver;

import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.driver.config.DriverConfigOption;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.TypedOption;
import org.apache.hugegraph.testutil.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ComputerOptionsTest extends UnitTestBase {
    private static Map<String, String> options;

    @BeforeClass
    public static void setup() {
        options = new HashMap<>();
        options.put(ComputerOptions.JOB_ID.name(), "local_002");
        options.put(ComputerOptions.JOB_WORKERS_COUNT.name(), "1");
        options.put(ComputerOptions.ALGORITHM_RESULT_CLASS.name(),
                    LongValue.class.getName());
        options.put(ComputerOptions.BSP_ETCD_ENDPOINTS.name(),
                    "http://abc:8098");
        options.put(ComputerOptions.HUGEGRAPH_URL.name(),
                    "http://127.0.0.1:8080");
    }

    @Test
    public void testDriverConfigOption() {
        DriverConfigOption<String> option = new DriverConfigOption<>(
                                                "test", "desc",
                                                disallowEmpty(),
                                                String.class);

        Assert.assertThrows(ConfigException.class, () -> {
            option.checkVal("");
        });

        Assert.assertNull(option.parseConvert(null));
    }

    @Test
    public void testOptions() {
        MapConfiguration mapConfig = new MapConfiguration(options);
        HugeConfig config = new HugeConfig(mapConfig);

        Map<String, TypedOption<?, ?>> allOptions = ComputerOptions.instance()
                                                                   .options();
        Collection<TypedOption<?, ?>> typedOptions = allOptions.values();

        for (TypedOption<?, ?> typedOption : typedOptions) {
            Object value = config.get(typedOption);
            String key = typedOption.name();
            if (options.containsKey(key)) {
                Assert.assertEquals(value instanceof Class ? ((Class<?>) value).getName() :
                                    String.valueOf(value), options.get(key));
            } else {
                Assert.assertEquals(value, typedOption.defaultValue());
            }
        }
    }
}
