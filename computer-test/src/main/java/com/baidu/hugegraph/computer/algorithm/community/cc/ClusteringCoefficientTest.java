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

package com.baidu.hugegraph.computer.algorithm.community.cc;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.algorithm.community.trianglecount.TriangleCountValue;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;

import jersey.repackaged.com.google.common.collect.ImmutableMap;

public class ClusteringCoefficientTest extends AlgorithmTestBase {
    
    private static final String VERTX_LABEL = "tc_user";
    private static final String EDGE_LABEL = "tc_know";
    private static final String PROPERTY_KEY = "tc_weight";

    protected static final Map<String, Object> EXPECTED_RESULTS =
              ImmutableMap.of("tc_A", 0.6666667F, "tc_B", 1.0F,
                              "tc_C", 0.5F, "tc_D", 0.6666667F,
                              "tc_E", 1.0F);

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
        Vertex vA = graph.addVertex(T.label, VERTX_LABEL, T.id, "tc_A",
                                    PROPERTY_KEY, 1);
        Vertex vB = graph.addVertex(T.label, VERTX_LABEL, T.id, "tc_B",
                                    PROPERTY_KEY, 1);
        Vertex vC = graph.addVertex(T.label, VERTX_LABEL, T.id, "tc_C",
                                    PROPERTY_KEY, 1);
        Vertex vD = graph.addVertex(T.label, VERTX_LABEL, T.id, "tc_D",
                                    PROPERTY_KEY, 1);
        Vertex vE = graph.addVertex(T.label, VERTX_LABEL, T.id, "tc_E",
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
    public void testClusteringCoefficientValue() {
        TriangleCountValue value = new TriangleCountValue();
        value.count(10L);
        Assert.assertThrows(UnsupportedOperationException.class,
                            () -> value.assign(null));
        Assert.assertThrows(UnsupportedOperationException.class,
                            () -> value.compareTo(new TriangleCountValue()));

        TriangleCountValue copy = (TriangleCountValue) value.copy();
        Assert.assertEquals(10L, copy.count());
        Assert.assertNotSame(value.idList(), copy.idList());

        Assert.assertContains("10", value.toString());
    }

    @Test
    public void testClusteringCoefficient() throws InterruptedException {
        runAlgorithm(ClusteringCoefficientParams.class.getName(),
                     ComputerOptions.OUTPUT_CLASS.name(),
                     ClusteringCoefficientOutputTest.class.getName());
    }

    public static class ClusteringCoefficientOutputTest
                  extends ClusteringCoefficientOutput {
        @Override
        public Vertex constructHugeVertex(
               com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            Vertex result = super.constructHugeVertex(vertex);
            Float expected = (Float) EXPECTED_RESULTS.get(result.id());

            if (expected != null) {
                Assert.assertEquals(expected, result.property(super.name()));
            }
            return result;
        }
    }
}
