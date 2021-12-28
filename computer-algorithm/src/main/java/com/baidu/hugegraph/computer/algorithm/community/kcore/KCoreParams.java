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

package com.baidu.hugegraph.computer.algorithm.community.kcore;

import java.util.Map;

import com.baidu.hugegraph.computer.algorithm.AlgorithmParams;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;

public class KCoreParams implements AlgorithmParams {

    @Override
    public void setAlgorithmParameters(Map<String, String> params) {
        this.setIfAbsent(params, ComputerOptions.WORKER_COMPUTATION_CLASS,
                         KCore.class.getName());
        this.setIfAbsent(params, ComputerOptions.ALGORITHM_RESULT_CLASS,
                         KCoreValue.class.getName());
        this.setIfAbsent(params, ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                         BytesId.class.getName());
        this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                         KCoreOutput.class.getName());

        params.put(ComputerOptions.VERTEX_WITH_EDGES_BOTHDIRECTION.name(),
                   Boolean.TRUE.toString());
        params.put(ComputerOptions.BSP_MAX_SUPER_STEP.name(),
                   String.valueOf(Integer.MAX_VALUE));
    }
}
