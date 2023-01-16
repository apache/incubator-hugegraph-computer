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

package org.apache.hugegraph.computer.algorithm.community.trianglecount;

import java.util.Map;

import org.apache.hugegraph.computer.algorithm.AlgorithmTestBase;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.output.hg.HugeGraphIntOutput;
import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.constant.T;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.testutil.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TriangleCountTest extends AlgorithmTestBase {

    private static final String VERTX_LABEL = "tc_user";
    private static final String EDGE_LABEL = "tc_know";
    private static final String PROPERTY_KEY = "tc_weight";

    protected static final Map<Object, Integer> EXPECTED_RESULTS = ImmutableMap.of("tc_A", 2,
                                                                                   "tc_B", 1,
                                                                                   "tc_C", 3,
                                                                                   "tc_D", 2,
                                                                                   "tc_E", 1);

    @BeforeClass
    public static void setup() {
        clearAll();

        SchemaManager schema = client().schema();
        schema.propertyKey(PROPERTY_KEY)
              .asInt()
              .ifNotExist()
              .create();
        schema.vertexLabel(VERTX_LABEL)
              .properties(PROPERTY_KEY)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL)
              .sourceLabel(VERTX_LABEL)
              .targetLabel(VERTX_LABEL)
              .properties(PROPERTY_KEY)
              .ifNotExist()
              .create();

        GraphManager graph = client().graph();
        Vertex vA = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "tc_A",
                                    PROPERTY_KEY, 1);
        Vertex vB = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "tc_B",
                                    PROPERTY_KEY, 1);
        Vertex vC = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "tc_C",
                                    PROPERTY_KEY, 1);
        Vertex vD = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "tc_D",
                                    PROPERTY_KEY, 1);
        Vertex vE = graph.addVertex(T.LABEL, VERTX_LABEL, T.ID, "tc_E",
                                    PROPERTY_KEY, 1);

        vA.addEdge(EDGE_LABEL, vB, PROPERTY_KEY, 1);
        vA.addEdge(EDGE_LABEL, vC, PROPERTY_KEY, 1);
        vB.addEdge(EDGE_LABEL, vC, PROPERTY_KEY, 1);
        vC.addEdge(EDGE_LABEL, vD, PROPERTY_KEY, 1);
        vD.addEdge(EDGE_LABEL, vA, PROPERTY_KEY, 1);
        vD.addEdge(EDGE_LABEL, vE, PROPERTY_KEY, 1);
        vE.addEdge(EDGE_LABEL, vD, PROPERTY_KEY, 1);
        vE.addEdge(EDGE_LABEL, vC, PROPERTY_KEY, 1);
    }

    @AfterClass
    public static void teardown() {
        clearAll();
    }

    @Test
    public void testRunAlgorithm() throws InterruptedException {
        runAlgorithm(TriangleCountParams.class.getName(),
                     ComputerOptions.OUTPUT_CLASS.name(),
                     TriangleCountIntOutputTest.class.getName());
    }

    @Test
    public void testTriangleCountValue() {
        TriangleCountValue value = new TriangleCountValue();
        value.count(10);
        Assert.assertThrows(UnsupportedOperationException.class,
                            () -> value.assign(null));
        Assert.assertThrows(UnsupportedOperationException.class,
                            () -> value.compareTo(new TriangleCountValue()));

        TriangleCountValue copy = value.copy();
        Assert.assertEquals(10, copy.count());
        Assert.assertNotSame(value.idSet(), copy.idSet());

        Assert.assertContains("10", value.toString());
    }

    public static class TriangleCountIntOutputTest extends HugeGraphIntOutput {

        @Override
        public Integer value(org.apache.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            Integer value = super.value(vertex);
            Integer expected = EXPECTED_RESULTS.get(vertex.id().string());
            if (expected != null) {
                Assert.assertEquals(expected, value);
            }

            return value;
        }
    }
}
