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

package com.baidu.hugegraph.computer.algorithm.community.trianglecount;

import java.util.Map;

import com.baidu.hugegraph.computer.algorithm.AlgorithmParams;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.EdgeFrequency;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.output.hg.IntHugeGraphOutput;
import com.baidu.hugegraph.structure.constant.WriteType;

public class TriangleCountParams implements AlgorithmParams {

    @Override
    public void setAlgorithmParameters(Map<String, String> params) {
        this.setIfAbsent(params, ComputerOptions.WORKER_COMPUTATION_CLASS,
                         TriangleCount.class.getName());
        this.setIfAbsent(params, ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                         IdList.class.getName());
        this.setIfAbsent(params, ComputerOptions.ALGORITHM_RESULT_CLASS,
                         TriangleCountValue.class.getName());
        this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                         IntHugeGraphOutput.class.getName());
        this.setIfAbsent(params, ComputerOptions.OUTPUT_RESULT_WRITE_TYPE,
                         WriteType.OLAP_RANGE.name());
        this.setIfAbsent(params, ComputerOptions.INPUT_EDGE_FREQ.name(),
                         EdgeFrequency.SINGLE.name());
    }
}
