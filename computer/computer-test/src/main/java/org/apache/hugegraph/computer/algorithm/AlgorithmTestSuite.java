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

package org.apache.hugegraph.computer.algorithm;

import org.apache.hugegraph.computer.algorithm.centrality.betweenness.BetweennessCentralityTest;
import org.apache.hugegraph.computer.algorithm.centrality.closeness.ClosenessCentralityTest;
import org.apache.hugegraph.computer.algorithm.centrality.degree.DegreeCentralityTest;
import org.apache.hugegraph.computer.algorithm.centrality.pagerank.PageRankTest;
import org.apache.hugegraph.computer.algorithm.community.cc.ClusteringCoefficientTest;
import org.apache.hugegraph.computer.algorithm.community.kcore.KcoreTest;
import org.apache.hugegraph.computer.algorithm.community.lpa.LpaTest;
import org.apache.hugegraph.computer.algorithm.community.trianglecount.TriangleCountTest;
import org.apache.hugegraph.computer.algorithm.community.wcc.WccTest;
import org.apache.hugegraph.computer.algorithm.path.rings.RingsDetectionTest;
import org.apache.hugegraph.computer.algorithm.path.rings.RingsDetectionWithFilterTest;
import org.apache.hugegraph.computer.algorithm.path.shortest.SingleSourceShortestPathTest;
import org.apache.hugegraph.computer.algorithm.sampling.RandomWalkTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

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
    BetweennessCentralityTest.class,
    RandomWalkTest.class,
    SingleSourceShortestPathTest.class
})
public class AlgorithmTestSuite {
}
