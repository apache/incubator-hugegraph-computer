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

package com.baidu.hugegraph.computer.algorithm.path.links;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.output.LimitedLogOutput;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class LinksTest extends AlgorithmTestBase {

    private static final String PROPERTY_WEIGHT = "weight";
    private static final String LABEL_USER = "user";
    private static final String LABEL_PAY = "pay";
    private static final String LABEL_KNOW = "know";

    private static final Map<String, Set<String>> EXPECT_RESULT =
            ImmutableMap.of(
                "A", ImmutableSet.of("{A, SA>1>>SB, B}",
                                         "{B, SB>1>>SD, D, SD>1>>SA, A, " +
                                         "SA>1>>SB, B}",
                                         "{D, SD>1>>SA, A, SA>1>>SB, B}"),
                "E", ImmutableSet.of("{E, SE>1>>SD, D}",
                                         "{A, SA>1>>SC, C, SC>1>>SE, E, " +
                                         "SE>1>>SD, D}",
                                         "{C, SC>1>>SE, E, SE>1>>SD, D}")
            );

    @BeforeClass
    public static void setup() {
        clearAll();

        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.propertyKey(PROPERTY_WEIGHT)
              .asDouble()
              .ifNotExist()
              .create();
        schema.vertexLabel(LABEL_USER)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel(LABEL_PAY)
              .sourceLabel(LABEL_USER)
              .targetLabel(LABEL_USER)
              .properties(PROPERTY_WEIGHT)
              .ifNotExist()
              .create();
        schema.edgeLabel(LABEL_KNOW)
              .sourceLabel(LABEL_USER)
              .targetLabel(LABEL_USER)
              .properties(PROPERTY_WEIGHT)
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex vA = graph.addVertex(T.label, LABEL_USER, T.id, "A");
        Vertex vB = graph.addVertex(T.label, LABEL_USER, T.id, "B");
        Vertex vC = graph.addVertex(T.label, LABEL_USER, T.id, "C");
        Vertex vD = graph.addVertex(T.label, LABEL_USER, T.id, "D");
        Vertex vE = graph.addVertex(T.label, LABEL_USER, T.id, "E");
        Vertex vF = graph.addVertex(T.label, LABEL_USER, T.id, "F");

        graph.addEdge(vA, LABEL_KNOW, vC, PROPERTY_WEIGHT, 1);
        graph.addEdge(vA, LABEL_PAY, vC, PROPERTY_WEIGHT, 1);
        graph.addEdge(vA, LABEL_PAY, vB, PROPERTY_WEIGHT, 4);
        graph.addEdge(vB, LABEL_PAY, vD, PROPERTY_WEIGHT, 0.5);
        graph.addEdge(vD, LABEL_KNOW, vB, PROPERTY_WEIGHT, 5);
        graph.addEdge(vD, LABEL_PAY, vA, PROPERTY_WEIGHT, 1);
        graph.addEdge(vE, LABEL_PAY, vD, PROPERTY_WEIGHT, 4);
        graph.addEdge(vE, LABEL_KNOW, vD, PROPERTY_WEIGHT, 4);
        graph.addEdge(vC, LABEL_PAY, vE, PROPERTY_WEIGHT, 3);
        graph.addEdge(vC, LABEL_KNOW, vE, PROPERTY_WEIGHT, 2);
        graph.addEdge(vF, LABEL_PAY, vE, PROPERTY_WEIGHT, 5);

        LinksTestOutput.EXPECT_RESULT = EXPECT_RESULT;
    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void test() throws InterruptedException {
        String analyze = "{" +
                         "    \"start_vertexes\": [" +
                         "        \"A\"," +
                         "        \"B\"," +
                         "        \"C\"," +
                         "        \"D\"," +
                         "        \"E\"" +
                         "    ]," +
                         "    \"edge_end_condition\": {" +
                         "        \"label\": \"pay\"," +
                         "        \"property_filter\": \"double($element" +
                         ".weight) >= 4\"" +
                         "    }," +
                         "    \"edge_compare_condition\": {" +
                         "        \"label\": \"pay\"," +
                         "        \"property_filter\": \"$element.weight " +
                         "> $message.weight\"" +
                         "    }" +
                         "}";
        runAlgorithm(LinksTestParams.class.getName(),
                     Links.OPTION_ANALYZE_CONFIG, analyze);
    }

    public static class LinksTestParams extends LinksParams {

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                             LinksTestOutput.class.getName());
            super.setAlgorithmParameters(params);
        }
    }

    public static class LinksTestOutput extends LimitedLogOutput {

        public static Map<String, Set<String>> EXPECT_RESULT;

        @Override
        public void write(
               com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            LinksValue values = vertex.value();
            Set<String> result =
                        EXPECT_RESULT.getOrDefault(vertex.id().toString(),
                                                   new HashSet<>());
            Assert.assertEquals(result.size(), values.size());
            values.values().forEach(value -> {
                Assert.assertTrue(result.contains(value.toString()));
            });
        }
    }
}
