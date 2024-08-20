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

package org.apache.hugegraph.computer.algorithm.path.shortest;

import java.util.Map;

import org.apache.hugegraph.computer.algorithm.AlgorithmTestBase;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.constant.T;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.Log;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

public class SingleSourceShortestPathTest extends AlgorithmTestBase {

    public static final String VL = "city";
    public static final String EL = "road";
    public static final String PROPERTY_KEY = "distance";

    public static final String SOURCE_ID = "\"A\"";
    public static final String TARGET_ID = "\"E\"";
    public static final String SHORTEST_PATH = "[A, C, B, E]";
    public static final double TOTAL_WEIGHT = 28;


    @BeforeClass
    public static void setup() {
        clearAll();

        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.propertyKey(PROPERTY_KEY)
              .asDouble()
              .ifNotExist()
              .create();
        schema.vertexLabel(VL)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel(EL)
              .sourceLabel(VL)
              .targetLabel(VL)
              .properties(PROPERTY_KEY)
              .nullableKeys(PROPERTY_KEY)
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex vA = graph.addVertex(T.LABEL, VL, T.ID, "A");
        Vertex vB = graph.addVertex(T.LABEL, VL, T.ID, "B");
        Vertex vC = graph.addVertex(T.LABEL, VL, T.ID, "C");
        Vertex vD = graph.addVertex(T.LABEL, VL, T.ID, "D");
        Vertex vE = graph.addVertex(T.LABEL, VL, T.ID, "E");
        Vertex vF = graph.addVertex(T.LABEL, VL, T.ID, "F");

        Vertex vJ = graph.addVertex(T.LABEL, VL, T.ID, "J");
        Vertex vK = graph.addVertex(T.LABEL, VL, T.ID, "K");

        vA.addEdge(EL, vC, PROPERTY_KEY, 5);
        vA.addEdge(EL, vD, PROPERTY_KEY, 30);

        vB.addEdge(EL, vA, PROPERTY_KEY, 2);
        vB.addEdge(EL, vE, PROPERTY_KEY, 8);

        vC.addEdge(EL, vB, PROPERTY_KEY, 15);
        vC.addEdge(EL, vF, PROPERTY_KEY, 7);

        vE.addEdge(EL, vB, PROPERTY_KEY, 6);
        vE.addEdge(EL, vD, PROPERTY_KEY, 4);

        vF.addEdge(EL, vC, PROPERTY_KEY, 8);
        vF.addEdge(EL, vD, PROPERTY_KEY, 10);
        vF.addEdge(EL, vE, PROPERTY_KEY, 18);

        vJ.addEdge(EL, vK);
    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void testRunAlgorithm() throws InterruptedException {
        runAlgorithm(SingleSourceShortestPathTestParams.class.getName());
    }

    public static class SingleSourceShortestPathTestParams extends SingleSourceShortestPathParams {

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                             SingleSourceShortestPathTestOutput.class.getName());
            this.setIfAbsent(params, SingleSourceShortestPath.OPTION_SOURCE_ID, SOURCE_ID);
            this.setIfAbsent(params, SingleSourceShortestPath.OPTION_TARGET_ID, TARGET_ID);
            this.setIfAbsent(params, SingleSourceShortestPath.OPTION_WEIGHT_PROPERTY,
                             SingleSourceShortestPathTest.PROPERTY_KEY);

            super.setAlgorithmParameters(params);
        }
    }

    public static class SingleSourceShortestPathTestOutput extends SingleSourceShortestPathOutput {

        private static final Logger LOG = Log.logger(SingleSourceShortestPathTestOutput.class);

        @Override
        public String value(org.apache.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            String json = super.value(vertex);

            if (vertex.id().value().toString().equals(TARGET_ID)) {
                Map map = JsonUtil.fromJson(json, Map.class);

                LOG.info("source vertex to target vertex: {}, {}, " +
                         "shortest path: {}, total weight: {}",
                         SOURCE_ID, TARGET_ID,
                         map.get("path"), map.get("total_weight"));
                Assert.assertEquals(map.get("path"), SHORTEST_PATH);
                Assert.assertEquals(map.get("total_weight"), TOTAL_WEIGHT);
            }
            return json;
        }
    }
}
