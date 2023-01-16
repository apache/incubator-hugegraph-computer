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

import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.TypedOption;

public interface Config {

    HugeConfig hugeConfig();

    <R> R get(TypedOption<?, R> option);

    boolean getBoolean(String key, boolean defaultValue);

    int getInt(String key, int defaultValue);

    long getLong(String key, long defaultValue);

    double getDouble(String key, double defaultValue);

    String getString(String key, String defaultValue);

    <T> T createObject(ConfigOption<Class<?>> clazzOption);

    <T> T createObject(ConfigOption<Class<?>> clazzOption,
                       boolean requiredNotNull);

    Boolean outputVertexAdjacentEdges();

    Boolean outputVertexProperties();

    Boolean outputEdgeProperties();
}
