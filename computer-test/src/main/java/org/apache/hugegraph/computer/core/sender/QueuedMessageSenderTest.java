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

package org.apache.hugegraph.computer.core.sender;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.worker.MockComputation2;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class QueuedMessageSenderTest extends UnitTestBase {

    private Config config;

    @Before
    public void setup() {
        this.config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_002",
                ComputerOptions.JOB_WORKERS_COUNT, "2",
                ComputerOptions.JOB_PARTITIONS_COUNT, "2",
                ComputerOptions.TRANSPORT_SERVER_PORT, "8086",
                ComputerOptions.BSP_REGISTER_TIMEOUT, "30000",
                ComputerOptions.BSP_LOG_INTERVAL, "10000",
                ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                ComputerOptions.WORKER_COMPUTATION_CLASS,
                MockComputation2.class.getName()
        );
    }

    @Test
    public void testInitAndClose() {
        QueuedMessageSender sender = new QueuedMessageSender(this.config);
        sender.addWorkerClient(1, new MockTransportClient());
        sender.addWorkerClient(2, new MockTransportClient());
        sender.init();

        Thread sendExecutor = Whitebox.getInternalState(sender, "sendExecutor");
        Assert.assertTrue(ImmutableSet.of(Thread.State.NEW,
                                          Thread.State.RUNNABLE,
                                          Thread.State.WAITING)
                                      .contains(sendExecutor.getState()));

        sender.close();
        Assert.assertTrue(ImmutableSet.of(Thread.State.TERMINATED)
                                      .contains(sendExecutor.getState()));
    }
}
