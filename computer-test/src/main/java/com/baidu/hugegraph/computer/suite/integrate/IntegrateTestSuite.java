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

package com.baidu.hugegraph.computer.suite.integrate;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.Log;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    //SenderIntegrateTest.class
})
public class IntegrateTestSuite {

    private static final Logger LOG = Log.logger(IntegrateTestSuite.class);

    @BeforeClass
    public static void setup() {
        LOG.info("Setup for IntegrateTestSuite of hugegraph-computer");

        // Don't forget to register options
        OptionSpace.register("computer",
                             "com.baidu.hugegraph.computer.core.config." +
                             "ComputerOptions");
        OptionSpace.register("computer-rpc",
                             "com.baidu.hugegraph.config.RpcOptions");

        Whitebox.setInternalState(ComputerOptions.BSP_ETCD_ENDPOINTS,
                                  "defaultValue", "http://localhost:2579");
    }
}
