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

package com.baidu.hugegraph.computer.algorithm.path.subgraph;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class SubGraphMatchTest extends AlgorithmTestBase {

    @Before
    public void init() {
        clearAll();
    }

    @After
    public void teardown() {
        clearAll();
    }

    @Test
    public void testWithNoPropertyFilter() throws InterruptedException {

        SubGraphMatchTestOutput.EXPECT_RESULT =
                                ImmutableMap.of(
                                        "1", ImmutableSet.of("1, 2, 3, 4"),
                                        "4", ImmutableSet.of("1, 3, 4, 13"),
                                        "9", ImmutableSet.of("9, 10, 11, 12",
                                                             "3, 9, 11, 12"),
                                        "13", ImmutableSet.of("2, 3, 4, 13")
                                );

        String noExpressionConfig = "[" +
                                    "    {" +
                                    "        \"id\": \"A\"," +
                                    "        \"label\": \"person\"," +
                                    "        \"edges\": [" +
                                    "            {" +
                                    "                \"targetId\": \"B\"," +
                                    "                \"label\": \"knows\"" +
                                    "            }," +
                                    "            {" +
                                    "                \"targetId\": \"C\"," +
                                    "                \"label\": \"knows\"" +
                                    "            }" +
                                    "        ]" +
                                    "    }," +
                                    "    {" +
                                    "        \"id\": \"B\"," +
                                    "        \"label\": \"person\"," +
                                    "        \"edges\": [" +
                                    "            {" +
                                    "                \"targetId\": \"D\"," +
                                    "                \"label\": \"knows\"" +
                                    "            }" +
                                    "        ]" +
                                    "    }," +
                                    "    {" +
                                    "        \"id\": \"C\"," +
                                    "        \"label\": \"person\"" +
                                    "    }," +
                                    "    {" +
                                    "        \"id\": \"D\"," +
                                    "        \"label\": \"person\"," +
                                    "        \"edges\": [" +
                                    "            {" +
                                    "                \"targetId\": \"A\"," +
                                    "                \"label\": \"knows\"" +
                                    "            }" +
                                    "        ]" +
                                    "    }" +
                                    "]";

        this.loadData();

        runAlgorithm(SubGraphMatchTestParams.class.getName(),
                     SubGraphMatch.SUBGRAPH_OPTION, noExpressionConfig);
    }

    @Test
    public void testWithPropertyFilter() throws InterruptedException {

        SubGraphMatchTestOutput.EXPECT_RESULT =
                ImmutableMap.of(
                        "1", ImmutableSet.of("1, 2, 3, 4")
                );

        String expressionConfig = "[" +
                        "    {" +
                        "        \"id\": \"A\"," +
                        "        \"label\": \"person\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"B\"," +
                        "                \"label\": \"knows\"," +
                        "                \"property_filter\": \"$element" +
                        ".weight == 1\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"C\"," +
                        "                \"label\": \"knows\"," +
                        "                \"property_filter\": \"$element" +
                        ".weight == 1\"" +
                        "            }" +
                        "        ]," +
                        "        \"property_filter\": \"$element.weight == " +
                        "1\"" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"B\"," +
                        "        \"label\": \"person\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"D\"," +
                        "                \"label\": \"knows\"," +
                        "                \"property_filter\": \"$element" +
                        ".weight == 1\"" +
                        "            }" +
                        "        ]," +
                        "        \"property_filter\": \"$element.weight == " +
                        "1\"" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"C\"," +
                        "        \"label\": \"person\"," +
                        "        \"property_filter\": \"$element.weight == " +
                        "1\"" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"D\"," +
                        "        \"label\": \"person\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"A\"," +
                        "                \"label\": \"knows\"," +
                        "                \"property_filter\": \"$element" +
                        ".weight == 1\"" +
                        "            }" +
                        "        ]," +
                        "        \"property_filter\": \"$element.weight == " +
                        "1\"" +
                        "    }" +
                        "]";

        this.loadData();

        runAlgorithm(SubGraphMatchTestParams.class.getName(),
                     SubGraphMatch.SUBGRAPH_OPTION, expressionConfig);
    }

    private void loadData() {

        final String PROPERTY_WEIGHT = "weight";
        final String VERTEX_LABEL = "person";
        final String EDGE_LABEL = "knows";

        SchemaManager schema = client().schema();

        schema.propertyKey(PROPERTY_WEIGHT)
              .asInt()
              .ifNotExist()
              .create();
        schema.vertexLabel(VERTEX_LABEL)
              .useCustomizeStringId()
              .properties(PROPERTY_WEIGHT)
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL)
              .sourceLabel(VERTEX_LABEL)
              .targetLabel(VERTEX_LABEL)
              .properties(PROPERTY_WEIGHT)
              .ifNotExist()
              .create();

        GraphManager graph = client().graph();
        Vertex v1 = graph.addVertex(T.label, VERTEX_LABEL, T.id, "1",
                                    PROPERTY_WEIGHT, 1);
        Vertex v2 = graph.addVertex(T.label, VERTEX_LABEL, T.id, "2",
                                    PROPERTY_WEIGHT, 1);
        Vertex v3 = graph.addVertex(T.label, VERTEX_LABEL, T.id, "3",
                                    PROPERTY_WEIGHT, 1);
        Vertex v4 = graph.addVertex(T.label, VERTEX_LABEL, T.id, "4",
                                    PROPERTY_WEIGHT, 1);
        Vertex v9 = graph.addVertex(T.label, VERTEX_LABEL, T.id, "9",
                                    PROPERTY_WEIGHT, 1);
        Vertex v10 = graph.addVertex(T.label, VERTEX_LABEL, T.id, "10",
                                     PROPERTY_WEIGHT, 1);
        Vertex v11 = graph.addVertex(T.label, VERTEX_LABEL, T.id, "11",
                                     PROPERTY_WEIGHT, 1);
        // Special property
        Vertex v12 = graph.addVertex(T.label, VERTEX_LABEL, T.id, "12",
                                     PROPERTY_WEIGHT, 2);
        Vertex v13 = graph.addVertex(T.label, VERTEX_LABEL, T.id, "13",
                                     PROPERTY_WEIGHT, 1);

        graph.addEdge(v1, EDGE_LABEL, v2, PROPERTY_WEIGHT, 1);
        graph.addEdge(v1, EDGE_LABEL, v3, PROPERTY_WEIGHT, 1);
        graph.addEdge(v2, EDGE_LABEL, v10, PROPERTY_WEIGHT, 1);
        graph.addEdge(v2, EDGE_LABEL, v12, PROPERTY_WEIGHT, 1);
        graph.addEdge(v3, EDGE_LABEL, v4, PROPERTY_WEIGHT, 1);
        graph.addEdge(v4, EDGE_LABEL, v1, PROPERTY_WEIGHT, 1);
        // Special property
        graph.addEdge(v4, EDGE_LABEL, v13, PROPERTY_WEIGHT, 2);
        graph.addEdge(v9, EDGE_LABEL, v3, PROPERTY_WEIGHT, 1);
        graph.addEdge(v9, EDGE_LABEL, v10, PROPERTY_WEIGHT, 1);
        graph.addEdge(v9, EDGE_LABEL, v11, PROPERTY_WEIGHT, 1);
        graph.addEdge(v11, EDGE_LABEL, v12, PROPERTY_WEIGHT, 1);
        graph.addEdge(v12, EDGE_LABEL, v9, PROPERTY_WEIGHT, 1);
        graph.addEdge(v13, EDGE_LABEL, v2, PROPERTY_WEIGHT, 1);
        graph.addEdge(v13, EDGE_LABEL, v3, PROPERTY_WEIGHT, 1);
    }

    @Test
    public void testWithMultiLabel() throws InterruptedException {

        final String LABEL_A = "A";
        final String LABEL_B = "B";
        final String LABEL_C = "C";
        final String LABEL_D = "D";
        final String EDGE_LABEL_A_B = "A_B";
        final String EDGE_LABEL_B_C = "B_C";
        final String EDGE_LABEL_C_D = "C_D";
        final String EDGE_LABEL_A_D = "A_D";
        final String EDGE_LABEL_B_D = "B_D";

        SchemaManager schema = client().schema();

        schema.vertexLabel(LABEL_A)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.vertexLabel(LABEL_B)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.vertexLabel(LABEL_C)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.vertexLabel(LABEL_D)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL_A_B)
              .sourceLabel(LABEL_A)
              .targetLabel(LABEL_B)
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL_B_C)
              .sourceLabel(LABEL_B)
              .targetLabel(LABEL_C)
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL_C_D)
              .sourceLabel(LABEL_C)
              .targetLabel(LABEL_D)
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL_A_D)
              .sourceLabel(LABEL_A)
              .targetLabel(LABEL_D)
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL_B_D)
              .sourceLabel(LABEL_B)
              .targetLabel(LABEL_D)
              .ifNotExist()
              .create();

        GraphManager graph = client().graph();
        Vertex vA = graph.addVertex(T.label, LABEL_A, T.id, "A");
        Vertex vB1 = graph.addVertex(T.label, LABEL_B, T.id, "B1");
        Vertex vB2 = graph.addVertex(T.label, LABEL_B, T.id, "B2");
        Vertex vC1 = graph.addVertex(T.label, LABEL_C, T.id, "C2");
        Vertex vC2 = graph.addVertex(T.label, LABEL_C, T.id, "C1");
        Vertex vD = graph.addVertex(T.label, LABEL_D, T.id, "D");

        graph.addEdge(vA, EDGE_LABEL_A_B, vB1);
        graph.addEdge(vA, EDGE_LABEL_A_B, vB2);
        graph.addEdge(vA, EDGE_LABEL_A_D, vD);
        graph.addEdge(vB1, EDGE_LABEL_B_C, vC1);
        graph.addEdge(vB2, EDGE_LABEL_B_C, vC2);
        graph.addEdge(vB1, EDGE_LABEL_B_D, vD);
        graph.addEdge(vB2, EDGE_LABEL_B_D, vD);
        graph.addEdge(vC1, EDGE_LABEL_C_D, vD);
        graph.addEdge(vC2, EDGE_LABEL_C_D, vD);

        String config = "[" +
                        "    {" +
                        "        \"id\": \"A\"," +
                        "        \"label\": \"A\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"D\"," +
                        "                \"label\": \"A_D\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"B\"," +
                        "                \"label\": \"A_B\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"B\"," +
                        "        \"label\": \"B\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"C\"," +
                        "                \"label\": \"B_C\"" +
                        "            }," +
                        "            {" +
                        "                \"targetId\": \"D\"," +
                        "                \"label\": \"B_D\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"C\"," +
                        "        \"label\": \"C\"," +
                        "        \"edges\": [" +
                        "            {" +
                        "                \"targetId\": \"D\"," +
                        "                \"label\": \"C_D\"" +
                        "            }" +
                        "        ]" +
                        "    }," +
                        "    {" +
                        "        \"id\": \"D\"," +
                        "        \"label\": \"D\"" +
                        "    }" +
                        "]";

        SubGraphMatchTestOutput.EXPECT_RESULT =
                                ImmutableMap.of(
                                "A", ImmutableSet.of("A, D, B1, C2",
                                                        "A, D, B2, C1")
                                );

        runAlgorithm(SubGraphMatchTestParams.class.getName(),
                     SubGraphMatch.SUBGRAPH_OPTION, config);
    }

    public static class SubGraphMatchTestParams extends SubGraphMatchParams {

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                             SubGraphMatchTestOutput.class.getName());
            super.setAlgorithmParameters(params);
        }
    }

    public static class SubGraphMatchTestOutput
                  extends SubGraphMatchHugeOutput {

        public static Map<String, Set<String>> EXPECT_RESULT;

        @Override
        public void write(
               com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            super.write(vertex);
            this.assertResult(vertex);
        }

        private void assertResult(
                com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            SubGraphMatchValue value = vertex.value();
            IdListList res = value.res();
            Set<String> expect =
                        EXPECT_RESULT.getOrDefault(vertex.id().toString(),
                                                   new HashSet<>());
            Assert.assertEquals(expect.size(), res.size());
            for (int i = 0; i < res.size(); i++) {
                IdList resItem = res.get(0);
                StringBuilder item = new StringBuilder();
                for (int j = 0; j < resItem.size(); j++) {
                    item.append(resItem.get(j).toString());
                    if (j != resItem.size() - 1) {
                        item.append(", ");
                    }
                }
                Assert.assertTrue(expect.contains(item.toString()));
            }
        }
    }
}
