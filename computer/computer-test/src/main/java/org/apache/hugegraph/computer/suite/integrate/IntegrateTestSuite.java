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

package org.apache.hugegraph.computer.suite.integrate;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.Log;
import org.apache.logging.log4j.LogManager;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    SenderIntegrateTest.class
})
public class IntegrateTestSuite {

    private static final Logger LOG = Log.logger(IntegrateTestSuite.class);

    @BeforeClass
    public static void setup() {
        Runtime.getRuntime().addShutdownHook(new Thread(LogManager::shutdown));

        LOG.info("Setup for IntegrateTestSuite of hugegraph-computer");

        // Don't forget to register options
        OptionSpace.register("computer",
                             "org.apache.hugegraph.computer.core.config.ComputerOptions");
        OptionSpace.register("computer-rpc", "org.apache.hugegraph.config.RpcOptions");

        String etcdUrl = System.getenv("BSP_ETCD_URL");
        if (StringUtils.isNotBlank(etcdUrl)) {
            Whitebox.setInternalState(ComputerOptions.BSP_ETCD_ENDPOINTS,
                                      "defaultValue", etcdUrl);
        }

    }
}
