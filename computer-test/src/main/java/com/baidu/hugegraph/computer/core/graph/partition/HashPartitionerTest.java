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
    public void testPartitionId() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "2"
        );
        Config config = ComputerContext.instance().config();
        HashPartitioner partitioner = new HashPartitioner(config);
        Id vertexId1 = new LongId(1L);
        Id vertexId2 = new LongId(2L);
        int partition1 = partitioner.partitionId(vertexId1);
        int partition2 = partitioner.partitionId(vertexId2);
        Assert.assertEquals(1, partition1);
        Assert.assertEquals(0, partition2);
        int workerId1 = partitioner.workerId(partition1);
        int workerId2 = partitioner.workerId(partition2);
        Assert.assertEquals(0, workerId1);
        Assert.assertEquals(0, workerId2);
    }

    @Test
    public void testWorkerId() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "2"
        );
        Config config = ComputerContext.instance().config();
        HashPartitioner partitioner = new HashPartitioner(config);
        int workerId1 = partitioner.workerId(1);
        int workerId2 = partitioner.workerId(2);
        Assert.assertEquals(0, workerId1);
        Assert.assertEquals(0, workerId2);
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
        Config config = ComputerContext.instance().config();
        HashPartitioner partitioner = new HashPartitioner(config);
        int[] partitionStat = new int[partitionCount];
        int[] workerStat = new int[workerCount];
        Random random = new Random(1001);
        for (int i = 0; i < 1024; i++) {
            int partitionId = partitioner.partitionId(
                              new LongId(random.nextLong()));
            partitionStat[partitionId]++;
            int workerId = partitioner.workerId(partitionId);
            workerStat[workerId]++;
        }
        LOG.info("partitionStat: {}", Arrays.toString(partitionStat));
        LOG.info("workerStat: {}", Arrays.toString(workerStat));
    }
}
