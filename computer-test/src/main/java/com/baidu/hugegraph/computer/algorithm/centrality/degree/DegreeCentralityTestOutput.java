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

package com.baidu.hugegraph.computer.algorithm.centrality.degree;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.LimitedLogOutput;
import com.google.common.collect.Streams;

public class DegreeCentralityTestOutput extends LimitedLogOutput {

    private String weight;

    public static boolean isRun;

    public DegreeCentralityTestOutput() {
        isRun = false;
    }

    @Override
    public void init(Config config, int partition) {
        super.init(config, partition);
        this.weight = config.getString(
                      DegreeCentrality.CONF_DEGREE_CENTRALITY_WEIGHT_PROPERTY,
                      "");
        isRun = false;
    }

    @Override
    public void write(Vertex vertex) {
        super.write(vertex);
        isRun = true;
        DoubleValue value = vertex.value();
        if (StringUtils.isEmpty(this.weight)) {
            Assert.assertEquals(vertex.numEdges(), value.value(), 0.000001);
        } else {
            Iterator<Edge> edges = vertex.edges().iterator();
            double totalValue = Streams.stream(edges).map(
                                (edge) -> ((DoubleValue) edge.properties()
                                                 .get(this.weight))
                                                 .value())
                                .reduce((v1, v2) -> v1 + v2)
                                .get();
            Assert.assertEquals(totalValue, value.value(), 0.000001);
        }
    }

    @Override
    public void close() {
        super.close();
    }

    public static void assertResult() {
        Assert.assertTrue(isRun);
    }
}
