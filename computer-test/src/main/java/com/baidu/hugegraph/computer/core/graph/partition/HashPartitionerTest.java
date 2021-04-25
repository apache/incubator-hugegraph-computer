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

import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.util.Log;

public class HashPartitionerTest {

    private static final Logger LOG = Log.logger(HashPartitionerTest.class);

    @Test
    public void test1Worker1Partition() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1"
        );
        // TODO: try to reduce call ComputerContext.instance() directly.
        Config config = ComputerContext.instance().config();
        Partitioner partitioner = config.createObject(
                                  ComputerOptions.WORKER_PARTITIONER);
        partitioner.init(config);
        Id vertexId1 = new LongId(1L);
        Id vertexId2 = new LongId(2L);
        Id vertexId3 = new LongId(-1L);
        Id vertexId4 = new LongId(-100L);
        Id vertexId5 = new LongId(Long.MIN_VALUE);
        Id vertexId6 = new LongId(Long.MAX_VALUE);

        int partition1 = partitioner.partitionId(vertexId1);
        int partition2 = partitioner.partitionId(vertexId2);
        int partition3 = partitioner.partitionId(vertexId3);
        int partition4 = partitioner.partitionId(vertexId4);
        int partition5 = partitioner.partitionId(vertexId5);
        int partition6 = partitioner.partitionId(vertexId6);
        Assert.assertEquals(0, partition1);
        Assert.assertEquals(0, partition2);
        Assert.assertEquals(0, partition3);
        Assert.assertEquals(0, partition4);
        Assert.assertEquals(0, partition5);
        Assert.assertEquals(0, partition6);

        int workerId1 = partitioner.workerId(partition1);
        int workerId2 = partitioner.workerId(partition2);
        int workerId3 = partitioner.workerId(partition3);
        int workerId4 = partitioner.workerId(partition4);
        int workerId5 = partitioner.workerId(partition5);
        int workerId6 = partitioner.workerId(partition6);
        Assert.assertEquals(0, workerId1);
        Assert.assertEquals(0, workerId2);
        Assert.assertEquals(0, workerId3);
        Assert.assertEquals(0, workerId4);
        Assert.assertEquals(0, workerId5);
        Assert.assertEquals(0, workerId6);
    }

    @Test
    public void test1Worker2Partition() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "2"
        );
        // TODO: try to reduce call ComputerContext.instance() directly.
        Config config = ComputerContext.instance().config();
        Partitioner partitioner = config.createObject(
                                  ComputerOptions.WORKER_PARTITIONER);
        partitioner.init(config);
        Id vertexId1 = new LongId(1L);
        Id vertexId2 = new LongId(2L);
        Id vertexId3 = new LongId(-1L);
        Id vertexId4 = new LongId(-100L);
        Id vertexId5 = new LongId(Long.MIN_VALUE);
        Id vertexId6 = new LongId(Long.MAX_VALUE);

        int partition1 = partitioner.partitionId(vertexId1);
        int partition2 = partitioner.partitionId(vertexId2);
        int partition3 = partitioner.partitionId(vertexId3);
        int partition4 = partitioner.partitionId(vertexId4);
        int partition5 = partitioner.partitionId(vertexId5);
        int partition6 = partitioner.partitionId(vertexId6);
        Assert.assertEquals(1, partition1);
        Assert.assertEquals(0, partition2);
        Assert.assertEquals(0, partition3);
        Assert.assertEquals(1, partition4);
        Assert.assertEquals(0, partition5);
        Assert.assertEquals(0, partition6);

        int workerId1 = partitioner.workerId(partition1);
        int workerId2 = partitioner.workerId(partition2);
        int workerId3 = partitioner.workerId(partition3);
        int workerId4 = partitioner.workerId(partition4);
        int workerId5 = partitioner.workerId(partition5);
        int workerId6 = partitioner.workerId(partition6);
        Assert.assertEquals(0, workerId1);
        Assert.assertEquals(0, workerId2);
        Assert.assertEquals(0, workerId3);
        Assert.assertEquals(0, workerId4);
        Assert.assertEquals(0, workerId5);
        Assert.assertEquals(0, workerId6);
    }

    @Test
    public void test1Worker3Partition() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "3"
        );
        // TODO: try to reduce call ComputerContext.instance() directly.
        Config config = ComputerContext.instance().config();
        Partitioner partitioner = config.createObject(
                                  ComputerOptions.WORKER_PARTITIONER);
        partitioner.init(config);
        Id vertexId1 = new LongId(1L);
        Id vertexId2 = new LongId(2L);
        Id vertexId3 = new LongId(-1L);
        Id vertexId4 = new LongId(-100L);
        Id vertexId5 = new LongId(Long.MIN_VALUE);
        Id vertexId6 = new LongId(Long.MAX_VALUE);

        int partition1 = partitioner.partitionId(vertexId1);
        int partition2 = partitioner.partitionId(vertexId2);
        int partition3 = partitioner.partitionId(vertexId3);
        int partition4 = partitioner.partitionId(vertexId4);
        int partition5 = partitioner.partitionId(vertexId5);
        int partition6 = partitioner.partitionId(vertexId6);
        Assert.assertEquals(1, partition1);
        Assert.assertEquals(2, partition2);
        Assert.assertEquals(0, partition3);
        Assert.assertEquals(0, partition4);
        Assert.assertEquals(0, partition5);
        Assert.assertEquals(0, partition6);

        int workerId1 = partitioner.workerId(partition1);
        int workerId2 = partitioner.workerId(partition2);
        int workerId3 = partitioner.workerId(partition3);
        int workerId4 = partitioner.workerId(partition4);
        int workerId5 = partitioner.workerId(partition5);
        int workerId6 = partitioner.workerId(partition6);
        Assert.assertEquals(0, workerId1);
        Assert.assertEquals(0, workerId2);
        Assert.assertEquals(0, workerId3);
        Assert.assertEquals(0, workerId4);
        Assert.assertEquals(0, workerId5);
        Assert.assertEquals(0, workerId6);
    }

    @Test
    public void test3Worker1Partition() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_WORKERS_COUNT, "3",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1"
        );
        // TODO: try to reduce call ComputerContext.instance() directly.
        Config config = ComputerContext.instance().config();
        Partitioner partitioner = config.createObject(
                                  ComputerOptions.WORKER_PARTITIONER);
        partitioner.init(config);
        Id vertexId1 = new LongId(1L);
        Id vertexId2 = new LongId(2L);
        Id vertexId3 = new LongId(-1L);
        Id vertexId4 = new LongId(-100L);
        Id vertexId5 = new LongId(Long.MIN_VALUE);
        Id vertexId6 = new LongId(Long.MAX_VALUE);

        int partition1 = partitioner.partitionId(vertexId1);
        int partition2 = partitioner.partitionId(vertexId2);
        int partition3 = partitioner.partitionId(vertexId3);
        int partition4 = partitioner.partitionId(vertexId4);
        int partition5 = partitioner.partitionId(vertexId5);
        int partition6 = partitioner.partitionId(vertexId6);
        Assert.assertEquals(0, partition1);
        Assert.assertEquals(0, partition2);
        Assert.assertEquals(0, partition3);
        Assert.assertEquals(0, partition4);
        Assert.assertEquals(0, partition5);
        Assert.assertEquals(0, partition6);

        int workerId1 = partitioner.workerId(partition1);
        int workerId2 = partitioner.workerId(partition2);
        int workerId3 = partitioner.workerId(partition3);
        int workerId4 = partitioner.workerId(partition4);
        int workerId5 = partitioner.workerId(partition5);
        int workerId6 = partitioner.workerId(partition6);
        Assert.assertEquals(0, workerId1);
        Assert.assertEquals(0, workerId2);
        Assert.assertEquals(0, workerId3);
        Assert.assertEquals(0, workerId4);
        Assert.assertEquals(0, workerId5);
        Assert.assertEquals(0, workerId6);
    }

    @Test
    public void testDist() {
        int workerCount = 2;
        int partitionCount = 10;
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_WORKERS_COUNT,
                Integer.toString(workerCount),
                ComputerOptions.JOB_PARTITIONS_COUNT,
                Integer.toString(partitionCount)
        );
        // TODO: try to reduce call ComputerContext.instance() directly.
        Config config = ComputerContext.instance().config();
        Partitioner partitioner = config.createObject(
                                  ComputerOptions.WORKER_PARTITIONER);
        partitioner.init(config);
        int[] partitionStat = new int[partitionCount];
        int[] workerStat = new int[workerCount];
        Random random = new Random();
        int hashTimes = 1024;
        for (int i = 0; i < hashTimes; i++) {
            int partitionId = partitioner.partitionId(
                              new LongId(random.nextLong()));
            partitionStat[partitionId]++;
            int workerId = partitioner.workerId(partitionId);
            workerStat[workerId]++;
        }
        LOG.info("Partition distribution: {}", Arrays.toString(partitionStat));
        LOG.info("Worker distribution: {}", Arrays.toString(workerStat));
        Assert.assertEquals(hashTimes, Arrays.stream(partitionStat).sum());
        Assert.assertEquals(hashTimes, Arrays.stream(workerStat).sum());
    }
}
