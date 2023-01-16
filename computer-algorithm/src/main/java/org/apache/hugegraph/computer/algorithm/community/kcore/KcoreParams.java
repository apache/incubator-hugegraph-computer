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

package org.apache.hugegraph.computer.algorithm.community.kcore;

import java.util.Map;

import org.apache.hugegraph.computer.algorithm.AlgorithmParams;
import org.apache.hugegraph.computer.core.combiner.ValueMinCombiner;
import org.apache.hugegraph.computer.core.config.ComputerOptions;

public class KcoreParams implements AlgorithmParams {

    @Override
    public void setAlgorithmParameters(Map<String, String> params) {
        this.setIfAbsent(params, ComputerOptions.WORKER_COMPUTATION_CLASS,
                         Kcore.class.getName());
        this.setIfAbsent(params, ComputerOptions.ALGORITHM_RESULT_CLASS,
                         KcoreValue.class.getName());
        this.setIfAbsent(params, ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                         BYTESID_CLASS_NAME);
        this.setIfAbsent(params, ComputerOptions.WORKER_COMBINER_CLASS,
                         ValueMinCombiner.class.getName());
        this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                         HUGEGRAPH_ID_OUTPUT_CLASS_NAME);
    }
}
