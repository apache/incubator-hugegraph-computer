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

package com.baidu.hugegraph.computer.algorithm;

import java.util.Map;

import org.slf4j.Logger;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.util.Log;

public interface AlgorithmParams {

    Logger LOG = Log.logger(AlgorithmParams.class);

    /**
     * set algorithm's specific configuration
     * @param params
     */
    void setAlgorithmParameters(Map<String, String> params);

    default void setIfNotFound(Map<String, String> params, String key,
                               String value) {
        if (!params.keySet().contains(key)) {
            LOG.debug("Put parameters key={}, value={}", key, value);
            params.put(key, value);
        }
    }

    default void setIfNotFound(Map<String, String> params,
                               ConfigOption<?> keyOption,
                               String value) {
        this.setIfNotFound(params, keyOption.name(), value);
    }
}
