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

import org.apache.hugegraph.computer.algorithm.AlgorithmTestBase;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.output.hg.HugeGraphDoubleOutput;
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

public class BetweennessCentralityTest extends AlgorithmTestBase {

    private static final Map<String, Double> EXPECT_RESULTS =
                         ImmutableMap.<String, Double>builder()
                                     .put("0", 0D)
                                     .put("1", 4.666666666666D)
                                     .put("2", 8.0D)
                                     .put("3", 0.666666666666D)
                                     .put("4", 8.666666666666D)
                                     .put("5", 10.0D)
                                     .put("6", 0.0D)
                                     .put("7", 0.0D)
                                     .build();

    @BeforeClass
    public static void setup() {
        clearAll();

        HugeClient client = client();
        SchemaManager schema = client.schema();
        schema.vertexLabel("user")
              .useCustomizeStringId()
              .ifNotExist()
              .create();

        schema.edgeLabel("link")
              .sourceLabel("user")
              .targetLabel("user")
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex v0 = graph.addVertex(T.LABEL, "user", T.ID, "0");
        Vertex v1 = graph.addVertex(T.LABEL, "user", T.ID, "1");
        Vertex v2 = graph.addVertex(T.LABEL, "user", T.ID, "2");
        Vertex v3 = graph.addVertex(T.LABEL, "user", T.ID, "3");
        Vertex v4 = graph.addVertex(T.LABEL, "user", T.ID, "4");
        Vertex v5 = graph.addVertex(T.LABEL, "user", T.ID, "5");
        Vertex v6 = graph.addVertex(T.LABEL, "user", T.ID, "6");
        Vertex v7 = graph.addVertex(T.LABEL, "user", T.ID, "7");

        v0.addEdge("link", v1);
        v0.addEdge("link", v2);

        v1.addEdge("link", v0);
        v1.addEdge("link", v2);
        v1.addEdge("link", v5);

        v2.addEdge("link", v0);
        v2.addEdge("link", v1);
        v2.addEdge("link", v3);
        v2.addEdge("link", v4);

        v3.addEdge("link", v2);
        v3.addEdge("link", v4);
        v3.addEdge("link", v5);

        v4.addEdge("link", v2);
        v4.addEdge("link", v3);
        v4.addEdge("link", v5);
        v4.addEdge("link", v6);
        v4.addEdge("link", v7);

        v5.addEdge("link", v1);
        v5.addEdge("link", v3);
        v5.addEdge("link", v4);
        v5.addEdge("link", v6);
        v5.addEdge("link", v7);

        v6.addEdge("link", v4);
        v6.addEdge("link", v5);
        v6.addEdge("link", v7);

        v7.addEdge("link", v4);
        v7.addEdge("link", v5);
        v7.addEdge("link", v6);

    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void testRunAlgorithm() throws InterruptedException {
        runAlgorithm(BetweennessCentralityParams.class.getName(),
                     BetweennessCentrality.OPTION_SAMPLE_RATE, "1.0D",
                     ComputerOptions.BSP_MAX_SUPER_STEP.name(), "5",
                     ComputerOptions.OUTPUT_CLASS.name(),
                     BetweennessCentralityTestOutput.class.getName());

    }

    public static class BetweennessCentralityTestOutput extends HugeGraphDoubleOutput {

        @Override
        protected Double value(org.apache.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            Double result = super.value(vertex);
            Double expect = EXPECT_RESULTS.get(vertex.id().string());
            Assert.assertNotNull(expect);
            assertEquals(expect, result);
            return result;
        }
    }
}
