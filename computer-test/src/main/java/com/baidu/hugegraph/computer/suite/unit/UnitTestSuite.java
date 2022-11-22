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

package com.baidu.hugegraph.computer.suite.unit;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestSuite;
import com.baidu.hugegraph.computer.core.allocator.AllocatorTestSuite;
import com.baidu.hugegraph.computer.core.bsp.BspTestSuite;
import com.baidu.hugegraph.computer.core.combiner.CombinerTestSuite;
import com.baidu.hugegraph.computer.core.common.CommonTestSuite;
import com.baidu.hugegraph.computer.core.compute.ComputeTestSuite;
import com.baidu.hugegraph.computer.core.config.ConfigTestSuite;
import com.baidu.hugegraph.computer.core.graph.GraphTestSuite;
import com.baidu.hugegraph.computer.core.input.InputTestSuite;
import com.baidu.hugegraph.computer.core.io.IOTestSuite;
import com.baidu.hugegraph.computer.core.network.NetworkTestSuite;
import com.baidu.hugegraph.computer.core.receiver.ReceiverTestSuite;
import com.baidu.hugegraph.computer.core.sender.SenderTestSuite;
import com.baidu.hugegraph.computer.core.sort.sorter.SorterTestSuite;
import com.baidu.hugegraph.computer.core.sort.sorting.SortingTestSuite;
import com.baidu.hugegraph.computer.core.store.StoreTestSuite;
import com.baidu.hugegraph.computer.core.util.UtilTestSuite;
import com.baidu.hugegraph.computer.core.worker.WorkerTestSuite;
import com.baidu.hugegraph.computer.dist.ComputerDistTestSuite;
import com.baidu.hugegraph.computer.driver.DriverTestSuite;
import com.baidu.hugegraph.computer.k8s.K8sTestSuite;
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
