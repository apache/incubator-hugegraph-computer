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

package org.apache.hugegraph.computer.core.common;

import java.io.IOException;

import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class ContainerInfoTest {

    private static final String HOST1 = "localhost";
    private static final String HOST2 = "not-exist-host";

    @Test
    public void testConstructor() {
        ContainerInfo worker = new ContainerInfo(0, HOST1, 8001, 8002);
        Assert.assertEquals(0, worker.id());
        Assert.assertEquals(HOST1, worker.hostname());
        Assert.assertEquals(8001, worker.rpcPort());
        Assert.assertEquals(8002, worker.dataPort());
    }

    @Test
    public void testReadWrite() throws IOException {
        ContainerInfo oldWorker = new ContainerInfo(0, HOST1, 8001, 8002);
        ContainerInfo newWorker = new ContainerInfo();
        UnitTestBase.assertEqualAfterWriteAndRead(oldWorker, newWorker);
    }

    @Test
    public void testEquals() {
        ContainerInfo container1 = new ContainerInfo(0, HOST1, 8001, 8002);
        ContainerInfo container2 = new ContainerInfo(0, HOST1, 8001, 8002);
        ContainerInfo container3 = new ContainerInfo(0, HOST2, 8001, 8002);
        Assert.assertEquals(container1, container2);
        Assert.assertNotEquals(container1, container3);
        Assert.assertNotEquals(container1, new Object());
    }

    @Test
    public void testHashCode() {
        ContainerInfo container = new ContainerInfo(0, HOST1, 8001, 8002);
        Assert.assertEquals(Integer.hashCode(0), container.hashCode());
    }

    @Test
    public void testToString() {
        ContainerInfo container = new ContainerInfo(0, HOST1, 8001, 8002);
        String str = "ContainerInfo{\"id\":0,\"hostname\":\"localhost\"," +
                     "\"rpcPort\":8001,\"dataPort\":8002}";
        Assert.assertEquals(str, container.toString());
    }
}
