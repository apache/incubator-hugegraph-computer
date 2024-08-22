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

package org.apache.hugegraph.computer.core.graph;

import java.io.IOException;

import org.apache.hugegraph.computer.core.graph.partition.PartitionStat;
import org.apache.hugegraph.computer.core.receiver.MessageStat;
import org.apache.hugegraph.computer.core.worker.WorkerStat;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class SuperstepStatTest {

    @Test
    public void testIncreasePartitionStat() {
        SuperstepStat stat = new SuperstepStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L);
        partitionStat.mergeSendMessageStat(new MessageStat(5L, 6L));
        partitionStat.mergeRecvMessageStat(new MessageStat(7L, 8L));
        stat.increase(partitionStat);
        stat.increase(partitionStat);

        Assert.assertEquals(partitionStat.vertexCount() * 2L,
                            stat.vertexCount());
        Assert.assertEquals(partitionStat.edgeCount() * 2L, stat.edgeCount());

        Assert.assertEquals(partitionStat.finishedVertexCount() * 2L,
                            stat.finishedVertexCount());

        Assert.assertEquals(partitionStat.messageSendCount() * 2L,
                            stat.messageSendCount());
        Assert.assertEquals(partitionStat.messageSendBytes() * 2L,
                            stat.messageSendBytes());

        Assert.assertEquals(partitionStat.messageRecvCount() * 2L,
                            stat.messageRecvCount());
        Assert.assertEquals(partitionStat.messageRecvBytes() * 2L,
                            stat.messageRecvBytes());
    }

    @Test
    public void testIncreaseWorkerStat() {
        SuperstepStat stat = new SuperstepStat();
        PartitionStat partitionStat1 = new PartitionStat(1, 4L, 3L, 2L);
        partitionStat1.mergeSendMessageStat(new MessageStat(5L, 6L));
        partitionStat1.mergeRecvMessageStat(new MessageStat(7L, 8L));

        PartitionStat partitionStat2 = new PartitionStat(2, 14L, 13L, 12L);
        partitionStat2.mergeSendMessageStat(new MessageStat(15L, 16L));
        partitionStat2.mergeRecvMessageStat(new MessageStat(17L, 18L));

        WorkerStat workerStat = new WorkerStat();
        workerStat.add(partitionStat1);
        workerStat.add(partitionStat2);
        stat.increase(workerStat);

        Assert.assertEquals(18, stat.vertexCount());
        Assert.assertEquals(16, stat.edgeCount());

        Assert.assertEquals(14L, stat.finishedVertexCount());

        Assert.assertEquals(20L, stat.messageSendCount());
        Assert.assertEquals(22L, stat.messageSendBytes());

        Assert.assertEquals(24L, stat.messageRecvCount());
        Assert.assertEquals(26L, stat.messageRecvBytes());
    }

    @Test
    public void testReadWrite() throws IOException {
        SuperstepStat stat1 = new SuperstepStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L);
        partitionStat.mergeSendMessageStat(new MessageStat(5L, 6L));
        partitionStat.mergeRecvMessageStat(new MessageStat(7L, 8L));
        stat1.increase(partitionStat);
        stat1.increase(partitionStat);
        SuperstepStat stat1ReadObj = new SuperstepStat();
        UnitTestBase.assertEqualAfterWriteAndRead(stat1, stat1ReadObj);
    }

    @Test
    public void testEquals() {
        SuperstepStat stat1 = new SuperstepStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L);
        partitionStat.mergeSendMessageStat(new MessageStat(5L, 6L));
        partitionStat.mergeRecvMessageStat(new MessageStat(7L, 8L));
        stat1.increase(partitionStat);
        stat1.increase(partitionStat);
        SuperstepStat stat2 = new SuperstepStat();
        stat2.increase(partitionStat);
        stat2.increase(partitionStat);
        SuperstepStat stat3 = new SuperstepStat();

        Assert.assertEquals(stat1, stat2);
        Assert.assertNotEquals(stat1, stat3);
        Assert.assertNotEquals(stat1, new Object());

        stat1.inactivate();
        Assert.assertFalse(stat1.active());
        Assert.assertNotEquals(stat1, stat2);
    }

    @Test
    public void testHashCode() {
        SuperstepStat stat1 = new SuperstepStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L);
        partitionStat.mergeSendMessageStat(new MessageStat(5L, 6L));
        partitionStat.mergeRecvMessageStat(new MessageStat(7L, 8L));
        stat1.increase(partitionStat);
        stat1.increase(partitionStat);
        SuperstepStat stat2 = new SuperstepStat();
        stat2.increase(partitionStat);
        stat2.increase(partitionStat);
        SuperstepStat stat3 = new SuperstepStat();
        Assert.assertEquals(stat1.hashCode(), stat2.hashCode());
        Assert.assertNotEquals(stat1.hashCode(), stat3.hashCode());
    }

    @Test
    public void testActive() {
        SuperstepStat stat = new SuperstepStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L);
        partitionStat.mergeSendMessageStat(new MessageStat(5L, 6L));
        partitionStat.mergeRecvMessageStat(new MessageStat(7L, 8L));
        stat.increase(partitionStat);
        stat.increase(partitionStat);
        Assert.assertTrue(stat.active());
        stat.inactivate();
        Assert.assertFalse(stat.active());
    }

    @Test
    public void testToString() {
        SuperstepStat stat = new SuperstepStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L);
        partitionStat.mergeSendMessageStat(new MessageStat(5L, 6L));
        partitionStat.mergeRecvMessageStat(new MessageStat(7L, 8L));
        stat.increase(partitionStat);
        String str = "SuperstepStat{\"vertexCount\":4,\"edgeCount\":3,\"" +
                     "finishedVertexCount\":2,\"messageSendCount\":5,\"" +
                     "messageSendBytes\":6,\"messageRecvCount\":7,\"" +
                     "messageRecvBytes\":8,\"active\":true}";
        Assert.assertEquals(str, stat.toString());
    }
}
