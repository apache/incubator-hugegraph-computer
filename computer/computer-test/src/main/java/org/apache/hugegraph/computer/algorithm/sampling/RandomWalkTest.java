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

package org.apache.hugegraph.computer.algorithm.sampling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.computer.algorithm.AlgorithmTestBase;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.constant.T;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Log;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RandomWalkTest extends AlgorithmTestBase {

    private static final String PROPERTY_KEY = "frequency";

    private static final Map<String, List<String>> EXPECT_WALK_PATH =
            ImmutableMap.of(
                    "F", ImmutableList.of(
                            "[F, G]",
                            "[F, G]",
                            "[F, G]"),
                    "G", ImmutableList.of("[G]"),
                    "I", ImmutableList.of("[I]")
            );

    @BeforeClass
    public static void setup() {
        clearAll();

        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.propertyKey(PROPERTY_KEY)
              .asDouble()
              .ifNotExist()
              .create();
        schema.vertexLabel("user")
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel("know")
              .sourceLabel("user")
              .targetLabel("user")
              .properties(PROPERTY_KEY)
              .nullableKeys(PROPERTY_KEY)
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex vA = graph.addVertex(T.LABEL, "user", T.ID, "A");
        Vertex vB = graph.addVertex(T.LABEL, "user", T.ID, "B");
        Vertex vC = graph.addVertex(T.LABEL, "user", T.ID, "C");
        Vertex vD = graph.addVertex(T.LABEL, "user", T.ID, "D");
        Vertex vE = graph.addVertex(T.LABEL, "user", T.ID, "E");

        Vertex vI = graph.addVertex(T.LABEL, "user", T.ID, "I");

        Vertex vF = graph.addVertex(T.LABEL, "user", T.ID, "F");
        Vertex vG = graph.addVertex(T.LABEL, "user", T.ID, "G");

        vA.addEdge("know", vB, PROPERTY_KEY, 9);
        vA.addEdge("know", vC);
        vA.addEdge("know", vD, PROPERTY_KEY, 3);
        vB.addEdge("know", vC, PROPERTY_KEY, 2);
        vC.addEdge("know", vA);
        vC.addEdge("know", vE, PROPERTY_KEY, 2);
        vD.addEdge("know", vA, PROPERTY_KEY, 7);
        vD.addEdge("know", vC, PROPERTY_KEY, 1);
        vE.addEdge("know", vD, PROPERTY_KEY, 8);

        vF.addEdge("know", vG, PROPERTY_KEY, 5);
    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void testRunAlgorithm() throws InterruptedException {
        runAlgorithm(RandomWalkTestParams.class.getName());
    }

    public static class RandomWalkTestParams extends RandomWalkParams {

        private static Integer WALK_PER_NODE = 3;
        private static Integer WALK_LENGTH = 3;

        private static String WEIGHT_PROPERTY = PROPERTY_KEY;
        private static Double DEFAULT_WEIGHT = 1.0;
        private static Double MIN_WEIGHT_THRESHOLD = 3.0;
        private static Double MAX_WEIGHT_THRESHOLD = 7.0;

        private static Double RETURN_FACTOR = 2.0;
        private static Double INOUT_FACTOR = 1.0 / 2.0;

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                             RandomWalkTest.RandomWalkTestOutput.class.getName());
            this.setIfAbsent(params, RandomWalk.OPTION_WALK_PER_NODE,
                             WALK_PER_NODE.toString());
            this.setIfAbsent(params, RandomWalk.OPTION_WALK_LENGTH,
                             WALK_LENGTH.toString());

            this.setIfAbsent(params, RandomWalk.OPTION_WEIGHT_PROPERTY,
                             WEIGHT_PROPERTY);
            this.setIfAbsent(params, RandomWalk.OPTION_DEFAULT_WEIGHT,
                             DEFAULT_WEIGHT.toString());
            this.setIfAbsent(params, RandomWalk.OPTION_MIN_WEIGHT_THRESHOLD,
                             MIN_WEIGHT_THRESHOLD.toString());
            this.setIfAbsent(params, RandomWalk.OPTION_MAX_WEIGHT_THRESHOLD,
                             MAX_WEIGHT_THRESHOLD.toString());

            this.setIfAbsent(params, RandomWalk.OPTION_RETURN_FACTOR,
                             RETURN_FACTOR.toString());
            this.setIfAbsent(params, RandomWalk.OPTION_INOUT_FACTOR,
                             INOUT_FACTOR.toString());

            super.setAlgorithmParameters(params);
        }
    }

    public static class RandomWalkTestOutput extends RandomWalkOutput {

        private static final Logger LOG = Log.logger(RandomWalkTestOutput.class);

        @Override
        public List<String> value(
                org.apache.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            List<String> pathList = super.value(vertex);
            LOG.info("vertex: {}, walk path: {}", vertex.id(), pathList);

            this.assertResult(vertex.id(), pathList);
            return pathList;
        }

        private void assertResult(Id id, List<String> path) {
            Set<String> keys = RandomWalkTest.EXPECT_WALK_PATH.keySet();
            if (keys.contains(id.string())) {
                List<String> expect = RandomWalkTest.EXPECT_WALK_PATH
                        .getOrDefault(id.toString(), new ArrayList<>());
                Assert.assertEquals(expect, path);
            } else {
                Assert.assertEquals(RandomWalkTestParams.WALK_PER_NODE.intValue(), path.size());
            }
        }
    }
}
