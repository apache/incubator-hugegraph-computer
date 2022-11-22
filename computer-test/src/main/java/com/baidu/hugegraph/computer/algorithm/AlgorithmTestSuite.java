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

package com.baidu.hugegraph.computer.algorithm;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.baidu.hugegraph.computer.algorithm.centrality.betweenness.BetweennessCentralityTest;
import com.baidu.hugegraph.computer.algorithm.centrality.closeness.ClosenessCentralityTest;
import com.baidu.hugegraph.computer.algorithm.centrality.degree.DegreeCentralityTest;
import com.baidu.hugegraph.computer.algorithm.centrality.pagerank.PageRankTest;
import com.baidu.hugegraph.computer.algorithm.community.cc.ClusteringCoefficientTest;
import com.baidu.hugegraph.computer.algorithm.community.kcore.KcoreTest;
import com.baidu.hugegraph.computer.algorithm.community.lpa.LpaTest;
import com.baidu.hugegraph.computer.algorithm.community.trianglecount.TriangleCountTest;
import com.baidu.hugegraph.computer.algorithm.community.wcc.WccTest;
import com.baidu.hugegraph.computer.algorithm.path.rings.RingsDetectionTest;
import com.baidu.hugegraph.computer.algorithm.path.rings.RingsDetectionWithFilterTest;
import org.apache.hugegraph.config.OptionSpace;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    PageRankTest.class,
    DegreeCentralityTest.class,
    WccTest.class,
    LpaTest.class,
    KcoreTest.class,
    TriangleCountTest.class,
    RingsDetectionTest.class,
    RingsDetectionWithFilterTest.class,
    ClusteringCoefficientTest.class,
    ClosenessCentralityTest.class,
    BetweennessCentralityTest.class
})
public class AlgorithmTestSuite {

    @BeforeClass
    public static void setup() throws ClassNotFoundException {
        // Don't forget to register options
        OptionSpace.register("computer",
                             "com.baidu.hugegraph.computer.core.config." +
                             "ComputerOptions");
        OptionSpace.register("computer-rpc",
                             "org.apache.hugegraph.config.RpcOptions");
    }
}
