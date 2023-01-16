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

package org.apache.hugegraph.computer.algorithm.path.rings;

import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.computer.algorithm.AlgorithmTestBase;
import org.apache.hugegraph.computer.algorithm.path.rings.RingsDetectionTest.RingsDetectionTestOutput;
import org.apache.hugegraph.computer.algorithm.path.rings.filter.RingsDetectionWithFilter;
import org.apache.hugegraph.computer.algorithm.path.rings.filter.RingsDetectionWithFilterParams;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.constant.T;
import org.apache.hugegraph.structure.graph.Vertex;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class RingsDetectionWithFilterTest extends AlgorithmTestBase {

    private static final Map<String, Set<String>> EXPECT_RINGS =
            ImmutableMap.of(
                    "A", ImmutableSet.of("[A, C, A]",
                                         "[A, D, C, A]",
                                         "[A, B, C, A]"),
                    "B", ImmutableSet.of("[B, C, B]"),
                    "C", ImmutableSet.of("[C, D, C]")
            );

    @BeforeClass
    public static void setup() {
        clearAll();

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
        Vertex vA = graph.addVertex(T.LABEL, "user", T.ID, "A", "weight", 1);
        Vertex vB = graph.addVertex(T.LABEL, "user", T.ID, "B", "weight", 1);
        Vertex vC = graph.addVertex(T.LABEL, "user", T.ID, "C", "weight", 1);
        Vertex vD = graph.addVertex(T.LABEL, "user", T.ID, "D", "weight", 1);
        Vertex vE = graph.addVertex(T.LABEL, "user", T.ID, "E", "weight", 3);

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

        RingsDetectionTestOutput.EXPECT_RINGS = EXPECT_RINGS;
    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void testRunAlgorithm() throws InterruptedException {
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
                        "            \"property_filter\": \"$message" +
                        ".weight==$element.weight\"" +
                        "        }" +
                        "    ]" +
                        "}";
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
