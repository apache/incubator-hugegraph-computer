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

package com.baidu.hugegraph.computer.core.receiver.vertex;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.receiver.PartitionBufferUtil;
import com.baidu.hugegraph.computer.core.store.DataFileManager;
import com.baidu.hugegraph.config.RpcOptions;

public class VertexRecvPartitionTest extends UnitTestBase {

    @Test
    public void testVertexPartitionBuffer() {
        Config config = UnitTestBase.updateWithRequiredOptions(
            RpcOptions.RPC_REMOTE_URL, "127.0.0.1:8090",
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.BSP_LOG_INTERVAL, "30000",
            ComputerOptions.BSP_MAX_SUPER_STEP, "2",
            ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
            ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "1000"
        );
        FileUtils.deleteQuietly(new File("data_dir1"));
        FileUtils.deleteQuietly(new File("data_dir2"));
        DataFileManager dataFileManager = new DataFileManager();
        dataFileManager.init(config);
        VertexRecvPartition partition = new VertexRecvPartition(
                                          config,
                                          dataFileManager);
        Assert.assertEquals("vertex", partition.type());
        for (int i = 0; i < 25; i++) {
            PartitionBufferUtil.addBuffer(partition, 100);
        }

        List<String> files = partition.onDiskFiles();
        Assert.assertEquals(2,files.size());
        partition.waitSortBuffersSorted();
        partition.sortReceiveBuffersAndWaitSorted();
        Assert.assertEquals(3,files.size());
    }
}
