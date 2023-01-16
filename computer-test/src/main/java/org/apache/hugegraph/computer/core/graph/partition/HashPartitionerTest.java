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

import java.util.Arrays;
import java.util.Random;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.util.Log;
import org.junit.Test;
import org.slf4j.Logger;

public class HashPartitionerTest extends UnitTestBase {

    private static final Logger LOG = Log.logger(HashPartitionerTest.class);

    @Test
    public void test1Worker1Partition() {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1"
        );
        Partitioner partitioner = config.createObject(
                                  ComputerOptions.WORKER_PARTITIONER);
        partitioner.init(config);
        Id vertexId1 = BytesId.of(1L);
        Id vertexId2 = BytesId.of(2L);
        Id vertexId3 = BytesId.of(-1L);
        Id vertexId4 = BytesId.of(-100L);
        Id vertexId5 = BytesId.of(Long.MIN_VALUE);
        Id vertexId6 = BytesId.of(Long.MAX_VALUE);

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
        Assert.assertEquals(1, workerId1);
        Assert.assertEquals(1, workerId2);
        Assert.assertEquals(1, workerId3);
        Assert.assertEquals(1, workerId4);
        Assert.assertEquals(1, workerId5);
        Assert.assertEquals(1, workerId6);
    }

    @Test
    public void test1Worker2Partition() {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.JOB_PARTITIONS_COUNT, "2"
        );
        Partitioner partitioner = config.createObject(
                                  ComputerOptions.WORKER_PARTITIONER);
        partitioner.init(config);
        Id vertexId1 = BytesId.of(1L);
        Id vertexId2 = BytesId.of(2L);
        Id vertexId3 = BytesId.of(-1L);
        Id vertexId4 = BytesId.of(-100L);
        Id vertexId5 = BytesId.of(Long.MIN_VALUE);
        Id vertexId6 = BytesId.of(Long.MAX_VALUE);

        int partition1 = partitioner.partitionId(vertexId1);
        int partition2 = partitioner.partitionId(vertexId2);
        int partition3 = partitioner.partitionId(vertexId3);
        int partition4 = partitioner.partitionId(vertexId4);
        int partition5 = partitioner.partitionId(vertexId5);
        int partition6 = partitioner.partitionId(vertexId6);
        Assert.assertTrue(partition1 < 2);
        Assert.assertTrue(partition2 < 2);
        Assert.assertTrue(partition3 < 2);
        Assert.assertTrue(partition4 < 2);
        Assert.assertTrue(partition5 < 2);
        Assert.assertTrue(partition6 < 2);

        int workerId1 = partitioner.workerId(partition1);
        int workerId2 = partitioner.workerId(partition2);
        int workerId3 = partitioner.workerId(partition3);
        int workerId4 = partitioner.workerId(partition4);
        int workerId5 = partitioner.workerId(partition5);
        int workerId6 = partitioner.workerId(partition6);
        Assert.assertEquals(1, workerId1);
        Assert.assertEquals(1, workerId2);
        Assert.assertEquals(1, workerId3);
        Assert.assertEquals(1, workerId4);
        Assert.assertEquals(1, workerId5);
        Assert.assertEquals(1, workerId6);
    }

    @Test
    public void test1Worker3Partition() {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.JOB_PARTITIONS_COUNT, "3"
        );
        Partitioner partitioner = config.createObject(
                                  ComputerOptions.WORKER_PARTITIONER);
        partitioner.init(config);
        Id vertexId1 = BytesId.of(1L);
        Id vertexId2 = BytesId.of(2L);
        Id vertexId3 = BytesId.of(-1L);
        Id vertexId4 = BytesId.of(-100L);
        Id vertexId5 = BytesId.of(Long.MIN_VALUE);
        Id vertexId6 = BytesId.of(Long.MAX_VALUE);

        int partition1 = partitioner.partitionId(vertexId1);
        int partition2 = partitioner.partitionId(vertexId2);
        int partition3 = partitioner.partitionId(vertexId3);
        int partition4 = partitioner.partitionId(vertexId4);
        int partition5 = partitioner.partitionId(vertexId5);
        int partition6 = partitioner.partitionId(vertexId6);
        Assert.assertTrue(partition1 < 3);
        Assert.assertTrue(partition2 < 3);
        Assert.assertTrue(partition3 < 3);
        Assert.assertTrue(partition4 < 3);
        Assert.assertTrue(partition5 < 3);
        Assert.assertTrue(partition6 < 3);

        int workerId1 = partitioner.workerId(partition1);
        int workerId2 = partitioner.workerId(partition2);
        int workerId3 = partitioner.workerId(partition3);
        int workerId4 = partitioner.workerId(partition4);
        int workerId5 = partitioner.workerId(partition5);
        int workerId6 = partitioner.workerId(partition6);
        Assert.assertEquals(1, workerId1);
        Assert.assertEquals(1, workerId2);
        Assert.assertEquals(1, workerId3);
        Assert.assertEquals(1, workerId4);
        Assert.assertEquals(1, workerId5);
        Assert.assertEquals(1, workerId6);
    }

    @Test
    public void test3Worker1Partition() {
        Assert.assertThrows(ComputerException.class, () -> {
            UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_WORKERS_COUNT, "3",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1"
            );
        }, e -> {
            Assert.assertTrue(e.getMessage().contains(
                              "The partitions count must be >= workers count"));
        });
    }

    @Test
    public void testDist() {
        int workerCount = 2;
        int partitionCount = 10;
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_WORKERS_COUNT, Integer.toString(workerCount),
            ComputerOptions.JOB_PARTITIONS_COUNT,
            Integer.toString(partitionCount)
        );
        Partitioner partitioner = config.createObject(
                                  ComputerOptions.WORKER_PARTITIONER);
        partitioner.init(config);
        int[] partitionStat = new int[partitionCount];
        int[] workerStat = new int[workerCount];
        Random random = new Random();
        int hashTimes = 1024;
        for (int i = 0; i < hashTimes; i++) {
            int partitionId = partitioner.partitionId(
                              BytesId.of(random.nextLong()));
            partitionStat[partitionId]++;
            int workerId = partitioner.workerId(partitionId);
            workerStat[--workerId]++;
        }
        LOG.info("Partition distribution: {}", Arrays.toString(partitionStat));
        LOG.info("Worker distribution: {}", Arrays.toString(workerStat));
        Assert.assertEquals(hashTimes, Arrays.stream(partitionStat).sum());
        Assert.assertEquals(hashTimes, Arrays.stream(workerStat).sum());
    }
}
