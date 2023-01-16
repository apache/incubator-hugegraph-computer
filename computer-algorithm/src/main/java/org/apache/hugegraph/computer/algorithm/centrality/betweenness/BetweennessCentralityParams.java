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

package org.apache.hugegraph.computer.algorithm.centrality.betweenness;

import java.util.Map;

import org.apache.hugegraph.computer.algorithm.AlgorithmParams;
import org.apache.hugegraph.computer.algorithm.centrality.closeness.ClosenessCentrality;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.master.DefaultMasterComputation;

public class BetweennessCentralityParams implements AlgorithmParams {

    @Override
    public void setAlgorithmParameters(Map<String, String> params) {
        this.setIfAbsent(params, ComputerOptions.MASTER_COMPUTATION_CLASS,
                         DefaultMasterComputation.class.getName());
        this.setIfAbsent(params, ComputerOptions.WORKER_COMPUTATION_CLASS,
                         BetweennessCentrality.class.getName());
        this.setIfAbsent(params, ComputerOptions.ALGORITHM_RESULT_CLASS,
                         BetweennessValue.class.getName());
        this.setIfAbsent(params, ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                         BetweennessMessage.class.getName());
        this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                         HUGEGRAPH_DOUBLE_OUTPUT_CLASS_NAME);
        this.setIfAbsent(params, ComputerOptions.INPUT_FILTER_CLASS,
                         EXTRACTALLPROPERTYINPUTFILTER_CLASS_NAME);
        this.setIfAbsent(params, ClosenessCentrality.OPTION_SAMPLE_RATE,
                         "0.5D");
    }
}
