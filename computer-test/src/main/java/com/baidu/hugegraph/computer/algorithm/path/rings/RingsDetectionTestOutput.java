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

package com.baidu.hugegraph.computer.algorithm.path.rings;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.testutil.Assert;

public class RingsDetectionTestOutput extends RingsDetectionOutput {

    public static Map<String, Set<String>> EXPECT_RINGS;

    @Override
    public void write(
           com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
        super.write(vertex);
        this.assertResult(vertex);
    }

    private void assertResult(
            com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
        IdListList rings = vertex.value();
        Set<String> expect =
                    EXPECT_RINGS.getOrDefault(vertex.id().toString(),
                                              new HashSet<>());

        Assert.assertEquals(expect.size(), rings.size());
        for (int i = 0; i < rings.size(); i++) {
            IdList ring = rings.get(0);
            StringBuilder ringValue = new StringBuilder();
            for (int j = 0; j < ring.size(); j++) {
                ringValue.append(ring.get(j).toString());
            }
            Assert.assertTrue(expect.contains(ringValue.toString()));
        }
    }
}
