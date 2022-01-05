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

package com.baidu.hugegraph.computer.algorithm.path.rings;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.algorithm.path.rings.RingsDetectionTest.RingsDetectionTestOutput;
import com.baidu.hugegraph.computer.algorithm.path.rings.filter.RingsDetectionWithFilter;
import com.baidu.hugegraph.computer.algorithm.path.rings.filter.RingsDetectionWithFilterParams;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class RingsDetectionWithFilterTest extends AlgorithmTestBase {

    @Before
    public void init() {
        clearAll();
    }

    @After
    public void teardown() {
        clearAll();
    }

    @Test
    public void testWithPropertyFilter() throws InterruptedException {
        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.propertyKey("weight")
              .asInt()
              .ifNotExist()
              .create();
        schema.vertexLabel("user")
              .properties("weight")
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel("know")
              .sourceLabel("user")
              .targetLabel("user")
              .properties("weight")
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex vA = graph.addVertex(T.label, "user", T.id, "A", "weight", 1);
        Vertex vB = graph.addVertex(T.label, "user", T.id, "B", "weight", 1);
        Vertex vC = graph.addVertex(T.label, "user", T.id, "C", "weight", 1);
        Vertex vD = graph.addVertex(T.label, "user", T.id, "D", "weight", 1);
        Vertex vE = graph.addVertex(T.label, "user", T.id, "E", "weight", 3);

        vA.addEdge("know", vB, "weight", 1);
        vA.addEdge("know", vC, "weight", 1);
        vA.addEdge("know", vD, "weight", 1);
        vB.addEdge("know", vA, "weight", 2);
        vB.addEdge("know", vC, "weight", 1);
        vB.addEdge("know", vE, "weight", 1);
        vC.addEdge("know", vA, "weight", 1);
        vC.addEdge("know", vB, "weight", 1);
        vC.addEdge("know", vD, "weight", 1);
        vD.addEdge("know", vC, "weight", 1);
        vD.addEdge("know", vE, "weight", 1);
        vE.addEdge("know", vC, "weight", 1);

        String filter = "{" +
                        "    \"vertex_filter\": [" +
                        "        {" +
                        "            \"label\": \"user\"," +
                        "            \"property_filter\": \"$element" +
                        ".weight==1\"" +
                        "        }" +
                        "    ]," +
                        "    \"edge_filter\": [" +
                        "        {" +
                        "            \"label\": \"know\"," +
                        "            \"property_filter\": \"$in" +
                        ".weight==$out.weight\"" +
                        "        }" +
                        "    ]" +
                        "}";

        RingsDetectionTestOutput.EXPECT_RINGS = ImmutableMap.of(
                "A", ImmutableSet.of("ACA", "ADCA", "ABCA"),
                "B", ImmutableSet.of("BCB"),
                "C", ImmutableSet.of("CDC")
        );

        runAlgorithm(RingsDetectionsTestParams.class.getName(),
                     RingsDetectionWithFilter.OPTION_FILTER, filter);
    }

    @Test
    public void testNoPropertyFilter() throws InterruptedException {
        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.vertexLabel("letter")
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.vertexLabel("number")
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel("letter-to-letter")
              .sourceLabel("letter")
              .targetLabel("letter")
              .ifNotExist()
              .create();
        schema.edgeLabel("number-to-number")
              .sourceLabel("number")
              .targetLabel("number")
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex vA = graph.addVertex(T.label, "letter", T.id, "A");
        Vertex vB = graph.addVertex(T.label, "letter", T.id, "B");
        Vertex vC = graph.addVertex(T.label, "letter", T.id, "C");
        Vertex v1 = graph.addVertex(T.label, "number", T.id, "1");
        Vertex v2 = graph.addVertex(T.label, "number", T.id, "2");
        Vertex v3 = graph.addVertex(T.label, "number", T.id, "3");

        vA.addEdge("letter-to-letter", vB);
        vA.addEdge("letter-to-letter", vC);
        vB.addEdge("letter-to-letter", vA);
        vC.addEdge("letter-to-letter", vB);
        v1.addEdge("number-to-number", v2);
        v2.addEdge("number-to-number", v3);
        v3.addEdge("number-to-number", v1);

        String filter = "{" +
                        "    \"vertex_filter\": [" +
                        "        {" +
                        "            \"label\": \"letter\"" +
                        "        }" +
                        "    ]" +
                        "}";
        RingsDetectionTestOutput.EXPECT_RINGS = ImmutableMap.of(
                "A", ImmutableSet.of("ABCA", "ABA")
        );
        runAlgorithm(RingsDetectionsTestParams.class.getName(),
                     RingsDetectionWithFilter.OPTION_FILTER, filter);

        filter = "{}";
        RingsDetectionTestOutput.EXPECT_RINGS = ImmutableMap.of(
                "A", ImmutableSet.of("ABCA", "ABA"),
                "1", ImmutableSet.of("1231")
        );
        runAlgorithm(RingsDetectionsTestParams.class.getName(),
                     RingsDetectionWithFilter.OPTION_FILTER, filter);
    }

    public static class RingsDetectionsTestParams
           extends RingsDetectionWithFilterParams {

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                             RingsDetectionTestOutput.class.getName());
            super.setAlgorithmParameters(params);
        }
    }
}
