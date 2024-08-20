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

package org.apache.hugegraph.computer.suite.unit;

import org.apache.hugegraph.computer.algorithm.AlgorithmTestSuite;
import org.apache.hugegraph.computer.core.allocator.AllocatorTestSuite;
import org.apache.hugegraph.computer.core.bsp.BspTestSuite;
import org.apache.hugegraph.computer.core.combiner.CombinerTestSuite;
import org.apache.hugegraph.computer.core.common.CommonTestSuite;
import org.apache.hugegraph.computer.core.compute.ComputeTestSuite;
import org.apache.hugegraph.computer.core.config.ConfigTestSuite;
import org.apache.hugegraph.computer.core.graph.GraphTestSuite;
import org.apache.hugegraph.computer.core.input.InputTestSuite;
import org.apache.hugegraph.computer.core.io.IOTestSuite;
import org.apache.hugegraph.computer.core.network.NetworkTestSuite;
import org.apache.hugegraph.computer.core.receiver.ReceiverTestSuite;
import org.apache.hugegraph.computer.core.sender.SenderTestSuite;
import org.apache.hugegraph.computer.core.sort.sorter.SorterTestSuite;
import org.apache.hugegraph.computer.core.sort.sorting.SortingTestSuite;
import org.apache.hugegraph.computer.core.store.StoreTestSuite;
import org.apache.hugegraph.computer.core.util.UtilTestSuite;
import org.apache.hugegraph.computer.core.worker.WorkerTestSuite;
import org.apache.hugegraph.computer.dist.ComputerDistTestSuite;
import org.apache.hugegraph.computer.driver.DriverTestSuite;
import org.apache.hugegraph.computer.k8s.K8sTestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    AllocatorTestSuite.class,
    CommonTestSuite.class,
    ConfigTestSuite.class,
    UtilTestSuite.class,
    BspTestSuite.class,
    CombinerTestSuite.class,
    GraphTestSuite.class,
    IOTestSuite.class,
    InputTestSuite.class,
    WorkerTestSuite.class,
    NetworkTestSuite.class,
    StoreTestSuite.class,
    SorterTestSuite.class,
    SortingTestSuite.class,
    SenderTestSuite.class,
    ReceiverTestSuite.class,
    ComputeTestSuite.class,
    ComputerDistTestSuite.class,
    DriverTestSuite.class,
    K8sTestSuite.class,
    AlgorithmTestSuite.class,
})
public class UnitTestSuite {
}
