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

package com.baidu.hugegraph.computer.core.graph.partition;

import java.io.IOException;

import org.junit.Test;

import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;

public class PartitionStatTest {

    @Test
    public void testConstructor() {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L);

        Assert.assertEquals(0, stat1.partitionId());
        Assert.assertEquals(1L, stat1.vertexCount());
        Assert.assertEquals(2L, stat1.edgeCount());

        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);
        Assert.assertEquals(1, stat2.partitionId());
        Assert.assertEquals(4L, stat2.vertexCount());
        Assert.assertEquals(3L, stat2.edgeCount());
        Assert.assertEquals(2L, stat2.finishedVertexCount());
        Assert.assertEquals(5L, stat2.messageCount());
        Assert.assertEquals(6L, stat2.messageBytes());
    }

    @Test
    public void testReadWrite() throws IOException {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L);
        PartitionStat stat1ReadObj = new PartitionStat();
        UnitTestBase.assertEqualAfterWriteAndRead(stat1, stat1ReadObj);
        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);
        PartitionStat stat2ReadObj = new PartitionStat();
        UnitTestBase.assertEqualAfterWriteAndRead(stat2, stat2ReadObj);
    }

    @Test
    public void testEquals() {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L);
        PartitionStat stat2 = new PartitionStat(0, 1L, 2L);
        PartitionStat stat3 = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);

        Assert.assertEquals(stat1, stat2);
        Assert.assertNotEquals(stat1, stat3);
        Assert.assertNotEquals(stat1, new Object());
    }

    @Test
    public void testHashCode() {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L);
        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);
        Assert.assertEquals(Integer.hashCode(0), stat1.hashCode());
        Assert.assertEquals(Integer.hashCode(1), stat2.hashCode());
    }

    @Test
    public void testToString() {
        PartitionStat stat = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);
        String str = "PartitionStat{\"partitionId\":1,\"vertexCount\":4,\"" +
                     "edgeCount\":3,\"finishedVertexCount\":2,\"" +
                     "messageCount\":5,\"messageBytes\":6}";
        Assert.assertEquals(str, stat.toString());
    }
}
