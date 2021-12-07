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

package com.baidu.hugegraph.computer.algorithm.centrality.degree;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.hg.HugeGraphDoubleOutput;
import com.google.common.collect.Streams;

public class DegreeCentralityTest extends AlgorithmTestBase {

    @Test
    public void testRunAlgorithm() throws InterruptedException {
        runAlgorithm(DegreeCentralityTestParams.class.getName(),
                     DegreeCentrality.OPTION_WEIGHT_PROPERTY, "rate");
        DegreeCentralityTestOutput.assertResult();

        runAlgorithm(DegreeCentralityTestParams.class.getName());
        DegreeCentralityTestOutput.assertResult();
    }

    public static class DegreeCentralityTestParams
                  extends DegreeCentralityParams {

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            params.put(ComputerOptions.OUTPUT_CLASS.name(),
                       DegreeCentralityTestOutput.class.getName());
            params.put(ComputerOptions.INPUT_SOURCE_TYPE.name(),
                       "hdfs");
            super.setAlgorithmParameters(params);
        }
    }

    public static class DegreeCentralityTestOutput
                  extends HugeGraphDoubleOutput {

        private String weight;
        private static boolean isRun;

        public DegreeCentralityTestOutput() {
        }

        @Override
        public void init(Config config, int partition) {
            super.init(config, partition);
            this.weight = config.getString(
                          DegreeCentrality.OPTION_WEIGHT_PROPERTY, "");
            isRun = false;
        }

        @Override
        public Double value(Vertex vertex) {
            Double value = super.value(vertex);
            isRun = true;
            if (StringUtils.isEmpty(this.weight)) {
                Assert.assertEquals(vertex.numEdges(), value, 0.000001);
            } else {
                Iterator<Edge> edges = vertex.edges().iterator();
                double totalValue = Streams.stream(edges).map(
                                    (edge) -> {
                                        DoubleValue weightValue =
                                                    edge.property(this.weight);
                                        if (weightValue == null) {
                                            return 1.0;
                                        } else {
                                            return weightValue.value();
                                        }
                                    }).reduce(Double::sum).orElse(0.0);
                Assert.assertEquals(totalValue, value, 0.000001);
            }
            return value;
        }

        public static void assertResult() {
            Assert.assertTrue(isRun);
        }
    }
}
