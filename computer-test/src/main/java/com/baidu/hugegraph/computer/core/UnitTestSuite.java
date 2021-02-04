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

package com.baidu.hugegraph.computer.core;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.allocator.AllocatorTestSuite;
import com.baidu.hugegraph.computer.core.bsp.BspTestSuite;
import com.baidu.hugegraph.computer.core.common.CommonTestSuite;
import com.baidu.hugegraph.computer.core.common.ExceptionTest;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.GraphTestSuite;
import com.baidu.hugegraph.computer.core.io.IOTestSuite;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.computer.core.worker.WorkerTestSuite;
import com.baidu.hugegraph.util.Log;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    AllocatorTestSuite.class,
    CommonTestSuite.class,
    BspTestSuite.class,
    ExceptionTest.class,
    GraphTestSuite.class,
    IOTestSuite.class,
    BspTestSuite.class,
    WorkerTestSuite.class
})
public class UnitTestSuite {

    private static final Logger LOG = Log.logger(UnitTestSuite.class);

    @BeforeClass
    public static void setup() {
        // Don't forget to register options
        OptionSpace.register("computer", ComputerOptions.instance());

        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_NAME, "test",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );
    }
}
