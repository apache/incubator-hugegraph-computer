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

package com.baidu.hugegraph.computer.algorithm.rings;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.computer.core.output.LimitedLogOutput;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RingsDetectionTest extends AlgorithmTestBase {

    private static final Map<String, List<List<String>>> EXPECT_RESULT =
            ImmutableMap.of(
                      "1", ImmutableList.of(
                              ImmutableList.of("1", "5", "2", "4", "3", "1")),
                      "2", ImmutableList.of(
                               ImmutableList.of("2", "4", "3", "2"),
                               ImmutableList.of("2", "4", "3", "5", "2"))
            );

    @BeforeClass
    public static void setup() {
        clearAll();

        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.propertyKey("id")
              .asText()
              .ifNotExist()
              .create();
        schema.propertyKey("name")
              .asText()
              .ifNotExist()
              .create();

        schema.vertexLabel("user")
              .properties("name")
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel("know")
              .sourceLabel("user")
              .targetLabel("user")
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex v1 = graph.addVertex(T.label, "user", "id", "1", "name", "v1");
        Vertex v2 = graph.addVertex(T.label, "user", "id", "2", "name", "v2");
        Vertex v3 = graph.addVertex(T.label, "user", "id", "3", "name", "v3");
        Vertex v4 = graph.addVertex(T.label, "user", "id", "4", "name", "v4");
        Vertex v5 = graph.addVertex(T.label, "user", "id", "5", "name", "v5");
        Vertex v6 = graph.addVertex(T.label, "user", "id", "6", "name", "v6");

        v1.addEdge("know", v5);
        v2.addEdge("know", v4);
        v4.addEdge("know", v3);
        v3.addEdge("know", v5);
        v5.addEdge("know", v2);
        v3.addEdge("know", v1);
        v6.addEdge("know", v1);
        v3.addEdge("know", v2);
    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void test() throws InterruptedException {
        runAlgorithm(RingsDetectionsTestParams.class.getName());
    }

    public static class RingsDetectionsTestParams extends RingsDetectionParams {

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                             RingsDetectionsTestOutput.class.getName());
            super.setAlgorithmParameters(params);
        }
    }

    public static class RingsDetectionsTestOutput extends LimitedLogOutput {

        @Override
        public void write(
               com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            super.write(vertex);

            IdListList rings = vertex.value();
            List<List<String>> expect = EXPECT_RESULT.get(
                                                      vertex.id().toString());

            this.assertResult(rings, expect);
        }

        private void assertResult(IdListList rings,
                                  List<List<String>> expectRings) {
            if (CollectionUtils.isEmpty(expectRings)) {
                Assert.assertEquals(0, rings.size());
            }

            for (int i = 0; i < rings.size(); i++) {
                IdList ring = rings.get(0);
                List<String> expectRing = expectRings.get(0);
                for (int j = 0; j < ring.size(); j++) {
                    Assert.assertEquals(expectRing.get(j),
                                        ring.get(j).toString());
                }
            }
        }
    }
}
