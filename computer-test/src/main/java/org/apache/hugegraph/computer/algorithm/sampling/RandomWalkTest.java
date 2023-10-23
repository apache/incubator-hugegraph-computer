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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RandomWalkTest extends AlgorithmTestBase {

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

        Vertex vI = graph.addVertex(T.LABEL, "user", T.ID, "I");

        Vertex vF = graph.addVertex(T.LABEL, "user", T.ID, "F");
        Vertex vG = graph.addVertex(T.LABEL, "user", T.ID, "G");

        vA.addEdge("know", vB);
        vA.addEdge("know", vC);
        vA.addEdge("know", vD);
        vB.addEdge("know", vC);
        vC.addEdge("know", vA);
        vC.addEdge("know", vE);
        vD.addEdge("know", vA);
        vD.addEdge("know", vC);
        vE.addEdge("know", vD);

        vF.addEdge("know", vG);
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

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                    RandomWalkTest.RandomWalkTestOutput.class.getName());
            this.setIfAbsent(params, RandomWalk.OPTION_WALK_PER_NODE,
                    WALK_PER_NODE.toString());
            this.setIfAbsent(params, RandomWalk.OPTION_WALK_LENGTH,
                    WALK_LENGTH.toString());

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
