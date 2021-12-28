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

package com.baidu.hugegraph.computer.algorithm.community.kcore;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;

public class KCoreTest extends AlgorithmTestBase {

    @Before
    public void setup() {
        clearAll();
    }

    @After
    public void teardown() {
        clearAll();
    }

    @Test
    public void test() throws InterruptedException {
        final String VERTEX_LABEL = "person";
        final String EDGE_LABEL = "knows";

        SchemaManager schema = client().schema();
        schema.vertexLabel(VERTEX_LABEL)
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel(EDGE_LABEL)
              .sourceLabel(VERTEX_LABEL)
              .targetLabel(VERTEX_LABEL)
              .ifNotExist()
              .create();

        GraphManager graph = client().graph();
        Vertex vA = graph.addVertex(T.label, VERTEX_LABEL, T.id, "A");
        Vertex vB = graph.addVertex(T.label, VERTEX_LABEL, T.id, "B");
        Vertex vC = graph.addVertex(T.label, VERTEX_LABEL, T.id, "C");
        Vertex vD = graph.addVertex(T.label, VERTEX_LABEL, T.id, "D");
        Vertex vE = graph.addVertex(T.label, VERTEX_LABEL, T.id, "E");
        Vertex vF = graph.addVertex(T.label, VERTEX_LABEL, T.id, "F");
        Vertex vG = graph.addVertex(T.label, VERTEX_LABEL, T.id, "G");
        Vertex vH = graph.addVertex(T.label, VERTEX_LABEL, T.id, "H");
        Vertex vI = graph.addVertex(T.label, VERTEX_LABEL, T.id, "I");
        Vertex vJ = graph.addVertex(T.label, VERTEX_LABEL, T.id, "J");
        Vertex vK = graph.addVertex(T.label, VERTEX_LABEL, T.id, "K");
        Vertex vL = graph.addVertex(T.label, VERTEX_LABEL, T.id, "L");
        Vertex vM = graph.addVertex(T.label, VERTEX_LABEL, T.id, "M");
        Vertex vN = graph.addVertex(T.label, VERTEX_LABEL, T.id, "N");

        vA.addEdge(EDGE_LABEL, vB);
        vA.addEdge(EDGE_LABEL, vC);
        vA.addEdge(EDGE_LABEL, vD);
        vB.addEdge(EDGE_LABEL, vA);
        vB.addEdge(EDGE_LABEL, vD);
        vB.addEdge(EDGE_LABEL, vN);
        vC.addEdge(EDGE_LABEL, vB);
        vD.addEdge(EDGE_LABEL, vC);
        vD.addEdge(EDGE_LABEL, vI);
        vE.addEdge(EDGE_LABEL, vG);
        vE.addEdge(EDGE_LABEL, vH);
        vF.addEdge(EDGE_LABEL, vE);
        vF.addEdge(EDGE_LABEL, vM);
        vF.addEdge(EDGE_LABEL, vJ);
        vG.addEdge(EDGE_LABEL, vH);
        vG.addEdge(EDGE_LABEL, vE);
        vH.addEdge(EDGE_LABEL, vG);
        vH.addEdge(EDGE_LABEL, vF);
        vH.addEdge(EDGE_LABEL, vJ);
        vJ.addEdge(EDGE_LABEL, vK);
        vL.addEdge(EDGE_LABEL, vJ);
        vM.addEdge(EDGE_LABEL, vJ);
        vN.addEdge(EDGE_LABEL, vE);

        Map<String, String> result = new HashMap<>();
        result.put("A", "4");
        result.put("B", "4");
        result.put("C", "3");
        result.put("D", "3");
        result.put("E", "3");
        result.put("G", "4");
        result.put("H", "3");
        KCoreTestOutput.EXPECT_RESULT = result;

        runAlgorithm(KCoreTestParams.class.getName());
    }

    public static class KCoreTestParams extends KCoreParams {

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS.name(),
                             KCoreTestOutput.class.getName());
            super.setAlgorithmParameters(params);
        }
    }

    public static class KCoreTestOutput extends KCoreOutput {

        public static Map<String, String> EXPECT_RESULT;

        @Override
        public void write(
               com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            KCoreValue value = vertex.value();
            if (EXPECT_RESULT.containsKey(vertex.id().string())) {
                Assert.assertEquals(EXPECT_RESULT.get(vertex.id().string()),
                                    value.string());
            }
            super.write(vertex);
        }
    }
}
