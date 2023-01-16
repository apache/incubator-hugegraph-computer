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

package org.apache.hugegraph.computer.algorithm.centrality.closeness;

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

public class ClosenessCentralityTest extends AlgorithmTestBase {

    @BeforeClass
    public static void setup() {
        clearAll();

        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.propertyKey("rate").asInt().ifNotExist().create();

        schema.vertexLabel("user")
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel("link")
              .sourceLabel("user")
              .targetLabel("user")
              .properties("rate")
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex vA = graph.addVertex(T.LABEL, "user", T.ID, "A");
        Vertex vB = graph.addVertex(T.LABEL, "user", T.ID, "B");
        Vertex vC = graph.addVertex(T.LABEL, "user", T.ID, "C");
        Vertex vD = graph.addVertex(T.LABEL, "user", T.ID, "D");
        Vertex vE = graph.addVertex(T.LABEL, "user", T.ID, "E");
        Vertex vF = graph.addVertex(T.LABEL, "user", T.ID, "F");

        vA.addEdge("link", vB, "rate", 1);
        vB.addEdge("link", vA, "rate", 1);

        vB.addEdge("link", vC, "rate", 2);
        vC.addEdge("link", vB, "rate", 2);

        vB.addEdge("link", vD, "rate", 2);
        vD.addEdge("link", vB, "rate", 2);

        vC.addEdge("link", vD, "rate", 1);
        vD.addEdge("link", vC, "rate", 1);

        vC.addEdge("link", vE, "rate", 3);
        vE.addEdge("link", vC, "rate", 3);

        vD.addEdge("link", vE, "rate", 1);
        vE.addEdge("link", vD, "rate", 1);

        vD.addEdge("link", vF, "rate", 4);
        vF.addEdge("link", vD, "rate", 4);

        vE.addEdge("link", vF, "rate", 2);
        vF.addEdge("link", vE, "rate", 2);
    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void testWithWeightProperty() throws InterruptedException {
        runAlgorithm(ClosenessCentralityParams.class.getName(),
                     ClosenessCentrality.OPTION_WEIGHT_PROPERTY, "rate",
                     ClosenessCentrality.OPTION_SAMPLE_RATE, "1.0D",
                     ComputerOptions.BSP_MAX_SUPER_STEP.name(), "5",
                     ComputerOptions.OUTPUT_CLASS.name(),
                     ClosenessWithWeightPropertyTestOutput.class.getName());
    }

    public static class ClosenessWithWeightPropertyTestOutput
           extends HugeGraphDoubleOutput {

        private final Map<String, Double> expectResults =
                ImmutableMap.<String, Double>builder()
                            .put("A", 2.083333333333333)
                            .put("B", 2.5333333333333337)
                            .put("C", 2.583333333333333)
                            .put("D", 3.1666666666666665)
                            .put("E", 2.583333333333333)
                            .put("F", 1.45)
                            .build();

        @Override
        protected Double value(
                  org.apache.hugegraph.computer.core.graph.vertex.Vertex
                  vertex) {
            Double result = super.value(vertex);
            Double expect = expectResults.get(vertex.id().string());
            Assert.assertNotNull(expect);
            assertEquals(expect, result);
            return result;
        }
    }

    @Test
    public void testWithoutWeightProperty() throws InterruptedException {
        runAlgorithm(ClosenessCentralityParams.class.getName(),
                     ClosenessCentrality.OPTION_SAMPLE_RATE, "1.0D",
                     ComputerOptions.BSP_MAX_SUPER_STEP.name(), "5",
                     ComputerOptions.OUTPUT_CLASS.name(),
                     ClosenessWithoutWeightPropertyTestOutput.class.getName());
    }

    public static class ClosenessWithoutWeightPropertyTestOutput
                  extends HugeGraphDoubleOutput {

        private final Map<String, Double> expectResults =
                ImmutableMap.<String, Double>builder()
                            .put("A", 2.6666666666666665)
                            .put("B", 4.0)
                            .put("C", 4.0)
                            .put("D", 4.5)
                            .put("E", 3.833333333333333)
                            .put("F", 3.333333333333333)
                            .build();

        @Override
        protected Double value(org.apache.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            Double result = super.value(vertex);
            Double expect = expectResults.get(vertex.id().string());
            Assert.assertNotNull(expect);
            assertEquals(expect, result);
            return result;
        }
    }
}
