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

package com.baidu.hugegraph.computer.algorithm.rank.pagerank;

import java.util.Map;

import com.baidu.hugegraph.computer.algorithm.AlgorithmParams;
import com.baidu.hugegraph.computer.core.combiner.DoubleValueSumCombiner;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.output.LimitLogOutput;

public class PageRankParams implements AlgorithmParams {

    @Override
    public void setAlgorithmParameters(Map<String, String> params) {
        this.setIfNotFound(params, ComputerOptions.MASTER_COMPUTATION_CLASS,
                           PageRankMasterComputation.class.getName());
        this.setIfNotFound(params, ComputerOptions.WORKER_COMPUTATION_CLASS,
                           PageRankComputation.class.getName());
        this.setIfNotFound(params, ComputerOptions.ALGORITHM_RESULT_CLASS,
                           DoubleValue.class.getName());
        this.setIfNotFound(params, ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                           DoubleValue.class.getName());
        this.setIfNotFound(params, ComputerOptions.WORKER_COMBINER_CLASS,
                           DoubleValueSumCombiner.class.getName());
        this.setIfNotFound(params, ComputerOptions.OUTPUT_CLASS,
                           LimitLogOutput.class.getName());
    }
}
