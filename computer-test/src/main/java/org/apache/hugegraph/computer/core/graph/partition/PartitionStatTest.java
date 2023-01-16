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

package org.apache.hugegraph.computer.core.graph.partition;

import java.io.IOException;

import org.apache.hugegraph.computer.core.receiver.MessageStat;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class PartitionStatTest {

    @Test
    public void testConstructor() {
        PartitionStat stat1 = new PartitionStat();

        Assert.assertEquals(0, stat1.partitionId());
        Assert.assertEquals(0L, stat1.vertexCount());
        Assert.assertEquals(0L, stat1.edgeCount());
        Assert.assertEquals(0L, stat1.finishedVertexCount());
        Assert.assertEquals(0L, stat1.messageSendCount());
        Assert.assertEquals(0L, stat1.messageSendBytes());
        Assert.assertEquals(0L, stat1.messageRecvCount());
        Assert.assertEquals(0L, stat1.messageRecvBytes());

        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L);
        stat2.mergeSendMessageStat(new MessageStat(5L, 6L));
        stat2.mergeRecvMessageStat(new MessageStat(7L, 8L));

        Assert.assertEquals(1, stat2.partitionId());
        Assert.assertEquals(4L, stat2.vertexCount());
        Assert.assertEquals(3L, stat2.edgeCount());
        Assert.assertEquals(2L, stat2.finishedVertexCount());
        Assert.assertEquals(5L, stat2.messageSendCount());
        Assert.assertEquals(6L, stat2.messageSendBytes());
        Assert.assertEquals(7L, stat2.messageRecvCount());
        Assert.assertEquals(8L, stat2.messageRecvBytes());
    }

    @Test
    public void testReadWrite() throws IOException {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L, 0L);
        PartitionStat stat1ReadObj = new PartitionStat();
        UnitTestBase.assertEqualAfterWriteAndRead(stat1, stat1ReadObj);

        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L);
        stat2.mergeSendMessageStat(new MessageStat(5L, 6L));
        stat2.mergeRecvMessageStat(new MessageStat(7L, 8L));

        PartitionStat stat2ReadObj = new PartitionStat();
        UnitTestBase.assertEqualAfterWriteAndRead(stat2, stat2ReadObj);
    }

    @Test
    public void testMerge() throws IOException {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L, 0L);
        stat1.mergeSendMessageStat(new MessageStat(5L, 6L));
        stat1.mergeRecvMessageStat(new MessageStat(7L, 8L));

        Assert.assertEquals(1L, stat1.vertexCount());
        Assert.assertEquals(2L, stat1.edgeCount());
        Assert.assertEquals(5L, stat1.messageSendCount());
        Assert.assertEquals(6L, stat1.messageSendBytes());
        Assert.assertEquals(7L, stat1.messageRecvCount());
        Assert.assertEquals(8L, stat1.messageRecvBytes());

        stat1.mergeSendMessageStat(new MessageStat(15L, 16L));
        stat1.mergeRecvMessageStat(new MessageStat(17L, 18L));
        Assert.assertEquals(20L, stat1.messageSendCount());
        Assert.assertEquals(22L, stat1.messageSendBytes());
        Assert.assertEquals(24L, stat1.messageRecvCount());
        Assert.assertEquals(26L, stat1.messageRecvBytes());

        MessageStat messageStat = new MessageStat(100L, 400L);
        stat1.mergeSendMessageStat(messageStat);
        stat1.mergeRecvMessageStat(messageStat);
        Assert.assertEquals(1L, stat1.vertexCount());
        Assert.assertEquals(2L, stat1.edgeCount());
        Assert.assertEquals(120L, stat1.messageSendCount());
        Assert.assertEquals(422L, stat1.messageSendBytes());
        Assert.assertEquals(124L, stat1.messageRecvCount());
        Assert.assertEquals(426L, stat1.messageRecvBytes());
    }

    @Test
    public void testEquals() {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L, 0L);
        PartitionStat stat2 = new PartitionStat(0, 1L, 2L, 0L);

        PartitionStat stat3 = new PartitionStat(1, 4L, 3L, 2L);
        stat3.mergeSendMessageStat(new MessageStat(5L, 6L));

        PartitionStat stat4 = new PartitionStat(1, 4L, 3L, 2L);
        stat4.mergeRecvMessageStat(new MessageStat(5L, 6L));

        PartitionStat stat30 = new PartitionStat(1, 4L, 3L, 2L);
        stat30.mergeSendMessageStat(new MessageStat(5L, 6L));

        PartitionStat stat40 = new PartitionStat(1, 4L, 3L, 2L);
        stat40.mergeRecvMessageStat(new MessageStat(5L, 6L));

        Assert.assertEquals(stat1, stat2);
        Assert.assertNotEquals(stat1, stat3);
        Assert.assertNotEquals(stat1, stat4);
        Assert.assertNotEquals(stat3, stat4);
        Assert.assertNotEquals(stat1, new Object());
        Assert.assertEquals(stat3, stat30);
        Assert.assertEquals(stat4, stat40);
    }

    @Test
    public void testHashCode() {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L, 0L);
        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L);
        stat2.mergeSendMessageStat(new MessageStat(5L, 6L));
        stat2.mergeRecvMessageStat(new MessageStat(7L, 8L));
        Assert.assertEquals(Integer.hashCode(0), stat1.hashCode());
        Assert.assertEquals(Integer.hashCode(1), stat2.hashCode());
    }

    @Test
    public void testToString() {
        PartitionStat stat = new PartitionStat(1, 4L, 3L, 2L);
        stat.mergeSendMessageStat(new MessageStat(5L, 6L));
        stat.mergeRecvMessageStat(new MessageStat(7L, 8L));
        String str = "PartitionStat{\"partitionId\":1,\"vertexCount\":4,\"" +
                     "edgeCount\":3,\"finishedVertexCount\":2," +
                     "\"messageSendCount\":5,\"messageSendBytes\":6," +
                     "\"messageRecvCount\":7,\"messageRecvBytes\":8}";
        Assert.assertEquals(str, stat.toString());
    }
}
