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

package com.baidu.hugegraph.computer.core.graph;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.BaseCoreTest;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.util.JsonUtil;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;

public class GraphStatTest {
    
    @Test
    public void testIncreasePartitionStat() {
        GraphStat stat = new GraphStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);
        stat.increase(partitionStat);
        stat.increase(partitionStat);

        Assert.assertEquals(partitionStat.vertexCount() * 2L,
                            stat.vertexCount());
        Assert.assertEquals(partitionStat.edgeCount() * 2L, stat.edgeCount());

        Assert.assertEquals(partitionStat.finishedVertexCount() * 2L,
                            stat.finishedVertexCount());
        Assert.assertEquals(partitionStat.messageCount() * 2L,
                            stat.messageCount());
        Assert.assertEquals(partitionStat.messageBytes() * 2L,
                            stat.messageBytes());
    }

    @Test
    public void testIncreasePartitionworkerStat() {
        GraphStat stat = new GraphStat();
        PartitionStat partitionStat1 = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);
        PartitionStat partitionStat2 = new PartitionStat(2, 4L, 3L, 2L, 5L, 6L);
        WorkerStat workerStat = new WorkerStat();
        workerStat.add(partitionStat1);
        workerStat.add(partitionStat2);
        stat.increase(workerStat);

        Assert.assertEquals(partitionStat1.vertexCount() * 2L,
                            stat.vertexCount());
        Assert.assertEquals(partitionStat1.edgeCount() * 2L, stat.edgeCount());

        Assert.assertEquals(partitionStat1.finishedVertexCount() * 2L,
                            stat.finishedVertexCount());
        Assert.assertEquals(partitionStat1.messageCount() * 2L,
                            stat.messageCount());
        Assert.assertEquals(partitionStat1.messageBytes() * 2L,
                            stat.messageBytes());
    }

    @Test
    public void testReadWrite() throws IOException {
        GraphStat stat1 = new GraphStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);
        stat1.increase(partitionStat);
        stat1.increase(partitionStat);
        GraphStat stat1ReadObj = new GraphStat();
        BaseCoreTest.assertEqualAfterWriteAndRead(stat1, stat1ReadObj);
    }

    @Test
    public void testEquals() {
        GraphStat stat1 = new GraphStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);
        stat1.increase(partitionStat);
        stat1.increase(partitionStat);
        GraphStat stat2 = new GraphStat();
        stat2.increase(partitionStat);
        stat2.increase(partitionStat);
        GraphStat stat3 = new GraphStat();

        Assert.assertEquals(stat1, stat2);
        Assert.assertNotEquals(stat1, stat3);
        Assert.assertNotEquals(stat1, new Object());

        stat1.halt(true);
        Assert.assertNotEquals(stat1, stat2);
    }

    @Test
    public void testHashCode() {
        GraphStat stat1 = new GraphStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);
        stat1.increase(partitionStat);
        stat1.increase(partitionStat);
        GraphStat stat2 = new GraphStat();
        stat2.increase(partitionStat);
        stat2.increase(partitionStat);
        GraphStat stat3 = new GraphStat();
        Assert.assertEquals(stat1.hashCode(), stat2.hashCode());
        Assert.assertNotEquals(stat1.hashCode(), stat3.hashCode());
    }

    @Test
    public void testToString() {
        GraphStat stat = new GraphStat();
        PartitionStat partitionStat = new PartitionStat(1, 4L, 3L, 2L, 5L, 6L);
        stat.increase(partitionStat);
        Assert.assertEquals(JsonUtil.toJson(stat), stat.toString());
    }
}
