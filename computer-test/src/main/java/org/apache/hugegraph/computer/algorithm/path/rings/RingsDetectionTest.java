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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hugegraph.computer.algorithm.AlgorithmTestBase;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.constant.T;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.testutil.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class RingsDetectionTest extends AlgorithmTestBase {

    private static final Map<String, Set<String>> EXPECT_RINGS =
            ImmutableMap.of(
                    "A", ImmutableSet.of("[A, B, C, A]",
                                         "[A, C, A]",
                                         "[A, B, C, E, D, A]",
                                         "[A, D, A]",
                                         "[A, D, C, A]",
                                         "[A, C, E, D, A]"),
                    "C", ImmutableSet.of("[C, E, D, C]")
            );

    @BeforeClass
    public static void setup() {
        clearAll();

        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.vertexLabel("user")
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel("know")
              .sourceLabel("user")
              .targetLabel("user")
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex vA = graph.addVertex(T.LABEL, "user", T.ID, "A");
        Vertex vB = graph.addVertex(T.LABEL, "user", T.ID, "B");
        Vertex vC = graph.addVertex(T.LABEL, "user", T.ID, "C");
        Vertex vD = graph.addVertex(T.LABEL, "user", T.ID, "D");
        Vertex vE = graph.addVertex(T.LABEL, "user", T.ID, "E");

        vA.addEdge("know", vB);
        vA.addEdge("know", vC);
        vA.addEdge("know", vD);
        vB.addEdge("know", vC);
        vC.addEdge("know", vA);
        vC.addEdge("know", vE);
        vD.addEdge("know", vA);
        vD.addEdge("know", vC);
        vE.addEdge("know", vD);

        RingsDetectionTestOutput.EXPECT_RINGS = EXPECT_RINGS;
    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void testRunAlgorithm() throws InterruptedException {
        runAlgorithm(RingsDetectionTestParams.class.getName());
    }

    public static class RingsDetectionTestParams extends RingsDetectionParams {

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                             RingsDetectionTestOutput.class.getName());
            super.setAlgorithmParameters(params);
        }
    }

    public static class RingsDetectionTestOutput extends RingsDetectionOutput {

        protected static Map<String, Set<String>> EXPECT_RINGS;

        @Override
        public List<String> value(
               org.apache.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            List<String> rings = super.value(vertex);
            this.assertResult(vertex.id(), rings);
            return rings;
        }

        private void assertResult(Id id, List<String> rings) {
            Set<String> expect = EXPECT_RINGS.getOrDefault(id.toString(),
                                                           new HashSet<>());

            rings = rings.stream()
                         .distinct()
                         .collect(Collectors.toList());

            Assert.assertEquals(expect.size(), rings.size());
            for (String ring : rings) {
                String error = "Expect: '" + ring + "' in " + expect;
                Assert.assertTrue(error, expect.contains(ring));
            }
        }
    }
}
