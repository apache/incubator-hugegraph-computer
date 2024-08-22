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

package org.apache.hugegraph.computer.algorithm.community.lpa;

import java.util.HashSet;

import org.apache.hugegraph.computer.algorithm.AlgorithmTestBase;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.output.hg.HugeGraphStringOutput;
import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.constant.T;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.testutil.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LpaTest extends AlgorithmTestBase {

    private static final String VERTX_LABEL = "tc_user";
    private static final String EDGE_LABEL = "tc_know";
    private static final String PROPERTY_KEY = "tc_weight";
    protected static  HashSet<String> communitySet = new HashSet<>();

    @BeforeClass
    public static void setup() {
        clearAll();

        SchemaManager schema = client().schema();

        schema.propertyKey(PROPERTY_KEY).asText().ifNotExist().create();

        schema.vertexLabel(VERTX_LABEL)
              .properties(PROPERTY_KEY)
              .enableLabelIndex(false)
              .useCustomizeStringId()
              .ifNotExist()
              .create();


        schema.edgeLabel(EDGE_LABEL)
              .sourceLabel(VERTX_LABEL)
              .targetLabel(VERTX_LABEL)
              .properties(PROPERTY_KEY)
              .enableLabelIndex(false)
              .ifNotExist()
              .create();

        GraphManager graph = client().graph();
        Vertex v0 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "0",
                                    PROPERTY_KEY, "0");
        Vertex v1 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "1",
                                    PROPERTY_KEY, "1");
        Vertex v2 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "2",
                                    PROPERTY_KEY, "2");
        Vertex v3 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "3",
                                    PROPERTY_KEY, "3");
        Vertex v4 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "4",
                                    PROPERTY_KEY, "4");
        Vertex v5 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "5",
                                    PROPERTY_KEY, "5");
        Vertex v6 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "6",
                                    PROPERTY_KEY, "6");
        Vertex v7 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "7",
                                    PROPERTY_KEY, "7");
        Vertex v8 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "8",
                                    PROPERTY_KEY, "8");
        Vertex v9 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "9",
                                    PROPERTY_KEY, "9");
        Vertex v10 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "10",
                                     PROPERTY_KEY, "10");
        Vertex v11 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "11",
                                     PROPERTY_KEY, "11");
        Vertex v12 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "12",
                                     PROPERTY_KEY, "12");
        Vertex v13 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "13",
                                     PROPERTY_KEY, "13");
        Vertex v14 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "14",
                                     PROPERTY_KEY, "14");
        Vertex v15 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "15",
                                     PROPERTY_KEY, "15");
        Vertex v16 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "16",
                                     PROPERTY_KEY, "16");
        Vertex v17 = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "17",
                                     PROPERTY_KEY, "17");

        v0.addEdge(EDGE_LABEL, v4, PROPERTY_KEY, "1");
        v0.addEdge(EDGE_LABEL, v7, PROPERTY_KEY, "1");
        v0.addEdge(EDGE_LABEL, v10, PROPERTY_KEY, "1");
        v0.addEdge(EDGE_LABEL, v11, PROPERTY_KEY, "1");
        v0.addEdge(EDGE_LABEL, v14, PROPERTY_KEY, "1");
        v0.addEdge(EDGE_LABEL, v16, PROPERTY_KEY, "1");
        v1.addEdge(EDGE_LABEL, v17, PROPERTY_KEY, "1");
        v2.addEdge(EDGE_LABEL, v5, PROPERTY_KEY, "1");
        v2.addEdge(EDGE_LABEL, v6, PROPERTY_KEY, "1");
        v2.addEdge(EDGE_LABEL, v8, PROPERTY_KEY, "1");
        v2.addEdge(EDGE_LABEL, v12, PROPERTY_KEY, "1");
        v3.addEdge(EDGE_LABEL, v9, PROPERTY_KEY, "1");
        v3.addEdge(EDGE_LABEL, v13, PROPERTY_KEY, "1");
        v9.addEdge(EDGE_LABEL, v15, PROPERTY_KEY, "1");
        v16.addEdge(EDGE_LABEL, v5, PROPERTY_KEY, "1");
    }

    @AfterClass
    public static void teardown() {
        clearAll();
    }

    @Test
    public void testRunAlgorithm() throws InterruptedException {
        runAlgorithm(LpaParams.class.getName(),
                     ComputerOptions.OUTPUT_CLASS.name(),
                     LpaTest.LpaIntOutputTest.class.getName());

        // check lpa result
        Assert.assertEquals(4, communitySet.size());
    }


    public static class LpaIntOutputTest extends HugeGraphStringOutput {

        @Override
        public String value(org.apache.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            String value = super.value(vertex);
            communitySet.add(value);
            return value;
        }
    }
}
