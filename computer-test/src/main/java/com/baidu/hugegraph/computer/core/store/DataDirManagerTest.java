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

package com.baidu.hugegraph.computer.core.store;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.config.RpcOptions;
import com.baidu.hugegraph.testutil.Assert;

public class DataDirManagerTest extends UnitTestBase {

    @Test
    public void testInitWithFile() throws IOException {
        File file = new File("exist");
        file.createNewFile();
        UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.WORKER_DATA_DIRS, "[" + file.getAbsolutePath() + "]"
        );
        Config config = context().config();
        DataFileManager dataFileManager = new DataFileManager();
        Assert.assertEquals(DataFileManager.NAME, dataFileManager.name());
        Assert.assertThrows(ComputerException.class, () -> {
            dataFileManager.init(config);
        }, e -> {
            Assert.assertContains("Can't create dir ", e.getMessage());
        });
        file.delete();
    }

    @Test
    public void testInitWithReadOnlyDir() throws IOException {
        UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.WORKER_DATA_DIRS, "[/etc]"
        );
        Config config = ComputerContext.instance().config();
        DataFileManager dataFileManager = new DataFileManager();
        Assert.assertThrows(ComputerException.class, () -> {
            dataFileManager.init(config);
        }, e -> {
            Assert.assertContains("Can't create dir", e.getMessage());
        });
    }

    @Test
    public void testNextDir() {
        UnitTestBase.updateWithRequiredOptions(
            RpcOptions.RPC_REMOTE_URL, "127.0.0.1:8090",
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.JOB_PARTITIONS_COUNT, "2",
            ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
            ComputerOptions.WORKER_RECEIVED_BUFFERS_SIZE_LIMIT, "300"
        );
        Config config = ComputerContext.instance().config();
        DataFileManager dataFileManager = new DataFileManager();

        dataFileManager.init(config);

        File dir1 = dataFileManager.nextDir();
        File dir2 = dataFileManager.nextDir();
        File dir3 = dataFileManager.nextDir();
        File dir4 = dataFileManager.nextDir();
        File dir5 = dataFileManager.nextDir();
        Assert.assertEquals(dir1, dir3);
        Assert.assertEquals(dir3, dir5);
        Assert.assertEquals(dir2, dir4);

        dataFileManager.close(config);
    }

    @Test
    public void testNextFile() {
        UnitTestBase.updateWithRequiredOptions(
            RpcOptions.RPC_REMOTE_URL, "127.0.0.1:8090",
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.JOB_PARTITIONS_COUNT, "2",
            ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
            ComputerOptions.WORKER_RECEIVED_BUFFERS_SIZE_LIMIT, "300"
        );
        Config config = ComputerContext.instance().config();
        DataFileManager dataFileManager = new DataFileManager();

        dataFileManager.init(config);

        File vertexFile = dataFileManager.nextFile("vertex", -1);
        File vertexSuperstepDir = vertexFile.getParentFile();
        Assert.assertEquals("-1", vertexSuperstepDir.getName());
        File vertexRootDir = vertexSuperstepDir.getParentFile();
        Assert.assertEquals("vertex", vertexRootDir.getName());

        File messageFile = dataFileManager.nextFile("message", 0);
        File messageSuperstepDir = messageFile.getParentFile();
        Assert.assertEquals("0", messageSuperstepDir.getName());
        File messageRootDir = messageSuperstepDir.getParentFile();
        Assert.assertEquals("message", messageRootDir.getName());

        File messageFile2 = dataFileManager.nextFile("message", 0);
        Assert.assertNotEquals(messageFile, messageFile2);

        dataFileManager.close(config);
    }
}
