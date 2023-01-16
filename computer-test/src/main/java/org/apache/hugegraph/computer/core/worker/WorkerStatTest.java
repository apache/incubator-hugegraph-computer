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

package org.apache.hugegraph.computer.core.worker;

import java.io.IOException;

import org.apache.hugegraph.computer.core.graph.partition.PartitionStat;
import org.apache.hugegraph.computer.core.receiver.MessageStat;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class WorkerStatTest {

    @Test
    public void testConstructor() {
        WorkerStat workerStat1 = new WorkerStat();
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L, 0L);
        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L);
        workerStat1.add(stat1);
        workerStat1.add(stat2);
        Assert.assertEquals(2, workerStat1.size());
        Assert.assertEquals(stat1, workerStat1.get(0));
        Assert.assertEquals(stat2, workerStat1.get(1));
        WorkerStat workerStat2 = new WorkerStat(1);
        Assert.assertEquals(1, workerStat2.workerId());
        Assert.assertEquals(0, workerStat2.size());
    }

    @Test
    public void testReadWrite() throws IOException {
        WorkerStat workerStat = new WorkerStat(1);
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L, 0L);
        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L);
        workerStat.add(stat1);
        workerStat.add(stat2);
        WorkerStat stats1ReadObj = new WorkerStat();
        UnitTestBase.assertEqualAfterWriteAndRead(workerStat, stats1ReadObj);
    }

    @Test
    public void testEquals() {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L, 0L);
        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L);
        WorkerStat workerStat1 = new WorkerStat();
        workerStat1.add(stat1);
        workerStat1.add(stat2);

        WorkerStat workerStat2 = new WorkerStat();
        workerStat2.add(stat1);
        workerStat2.add(stat2);

        WorkerStat workerStat3 = new WorkerStat();

        Assert.assertEquals(workerStat1, workerStat2);
        Assert.assertNotEquals(workerStat1, workerStat3);
        Assert.assertNotEquals(workerStat1, new Object());
    }

    @Test
    public void testHashCode() {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L, 0L);
        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L);
        WorkerStat workerStat1 = new WorkerStat(1);
        workerStat1.add(stat1);
        workerStat1.add(stat2);

        WorkerStat workerStat2 = new WorkerStat(1);
        workerStat2.add(stat1);
        workerStat2.add(stat2);

        WorkerStat workerStat3 = new WorkerStat(2);

        Assert.assertEquals(workerStat1.hashCode(), workerStat2.hashCode());
        Assert.assertNotEquals(workerStat1.hashCode(), workerStat3.hashCode());
    }

    @Test
    public void testToString() {
        PartitionStat stat1 = new PartitionStat(0, 1L, 2L, 0L);
        PartitionStat stat2 = new PartitionStat(1, 4L, 3L, 2L);
        stat2.mergeSendMessageStat(new MessageStat(5L, 6L));
        stat2.mergeRecvMessageStat(new MessageStat(7L, 8L));

        WorkerStat workerStat = new WorkerStat();
        workerStat.add(stat1);
        workerStat.add(stat2);
        String str = "WorkerStat{\"workerId\":0," +
                     "\"partitionStats\":[{\"partitionId\":0," +
                     "\"vertexCount\":1,\"edgeCount\":2,\"" +
                     "finishedVertexCount\":0," +
                     "\"messageSendCount\":0,\"messageSendBytes\":0," +
                     "\"messageRecvCount\":0,\"messageRecvBytes\":0}," +
                     "{\"partitionId\":1,\"vertexCount\":4," +
                     "\"edgeCount\":3,\"finishedVertexCount\":2," +
                     "\"messageSendCount\":5,\"messageSendBytes\":6," +
                     "\"messageRecvCount\":7,\"messageRecvBytes\":8}]}";
        Assert.assertEquals(str, workerStat.toString());
    }
}
