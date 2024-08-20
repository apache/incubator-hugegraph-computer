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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hugegraph.computer.algorithm.AlgorithmParams;
import org.apache.hugegraph.computer.core.allocator.Allocator;
import org.apache.hugegraph.computer.core.allocator.DefaultAllocator;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.config.DefaultConfig;
import org.apache.hugegraph.computer.core.graph.BuiltinGraphFactory;
import org.apache.hugegraph.computer.core.graph.GraphFactory;

public class ComputerContextUtil {

    public static Config initContext(Map<String, String> params) {
        // Set algorithm's parameters
        String algorithmParamsName = params.get(
               ComputerOptions.ALGORITHM_PARAMS_CLASS.name());
        AlgorithmParams algorithmParams;
        try {
            algorithmParams = (AlgorithmParams) Class.forName(
                              algorithmParamsName).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new ComputerException("Can't create algorithmParams, algorithmParamsName = %s",
                                        algorithmParamsName);
        }
        algorithmParams.setAlgorithmParameters(params);

        Config config = new DefaultConfig(params);
        GraphFactory graphFactory = new BuiltinGraphFactory();
        Allocator allocator = new DefaultAllocator(config, graphFactory);
        ComputerContext.initContext(config, graphFactory, allocator);
        return config;
    }

    public static void initContext(Properties properties) {
        initContext(convertToMap(properties));
    }

    public static Map<String, String> convertToMap(String... options) {
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

    public static Map<String, String> convertToMap(Properties properties) {
        if (properties == null) {
            throw new ComputerException("Properties can't be null");
        }

        Map<String, String> map = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            map.put(key, properties.getProperty(key));
        }

        return map;
    }
}
