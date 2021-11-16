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

package com.baidu.hugegraph.computer.core.combiner;

import java.util.Map;

import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.util.E;

public class MergeOldPropertiesCombiner implements PropertiesCombiner {

    /**
     * Merge properties v1 into v2. If a property exists in both v1 and v2,
     * remain the value in v2.
     */
    @Override
    public Properties combine(Properties v1, Properties v2) {
        E.checkArgumentNotNull(v1, "The combine parameter v1 can't be null");
        E.checkArgumentNotNull(v2, "The combine parameter v2 can't be null");
        Map<String, Value> v1Map = v1.get();
        for (Map.Entry<String, Value> entry : v1Map.entrySet()) {
            v2.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return v2;
    }
}
