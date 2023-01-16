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

import java.util.Map;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class MessageSendBuffersTest extends UnitTestBase {

    @Test
    public void testConstructor() {
        UnitTestBase.updateOptions(
                ComputerOptions.JOB_PARTITIONS_COUNT, "3",
                ComputerOptions.WORKER_WRITE_BUFFER_THRESHOLD, "100",
                ComputerOptions.WORKER_WRITE_BUFFER_THRESHOLD, "120"
        );

        MessageSendBuffers buffers = new MessageSendBuffers(
                ComputerContext.instance());
        Map<Integer, MessageSendPartition> innerBuffers = buffers.all();
        Assert.assertEquals(3, innerBuffers.size());
    }

    @Test
    public void testGetter() {
        UnitTestBase.updateOptions(
            ComputerOptions.JOB_PARTITIONS_COUNT, "3",
            ComputerOptions.WORKER_WRITE_BUFFER_THRESHOLD, "100",
            ComputerOptions.WORKER_WRITE_BUFFER_THRESHOLD, "120"
        );
        MessageSendBuffers buffers = new MessageSendBuffers(
                                     ComputerContext.instance());
        Assert.assertNotNull(buffers.get(0));
        Assert.assertNotNull(buffers.get(1));
        Assert.assertNotNull(buffers.get(2));
        Assert.assertThrows(ComputerException.class, () -> {
            buffers.get(3);
        }, e -> {
            Assert.assertTrue(e.getMessage().contains("Invalid partition id"));
        });
    }
}
