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

package org.apache.hugegraph.computer.algorithm.community.wcc;

import org.apache.hugegraph.computer.algorithm.AlgorithmTestBase;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.junit.Test;

public class WccTest extends AlgorithmTestBase {

    @Test
    public void testRunAlgorithm() throws InterruptedException {
        runAlgorithm(WccParams.class.getName(),
                     ComputerOptions.JOB_ID.name(), "local_wcc",
                     ComputerOptions.JOB_WORKERS_COUNT.name(), "1",
                     ComputerOptions.BSP_REGISTER_TIMEOUT.name(), "100000",
                     ComputerOptions.BSP_LOG_INTERVAL.name(), "30000",
                     ComputerOptions.BSP_MAX_SUPER_STEP.name(), "10");
    }
}
