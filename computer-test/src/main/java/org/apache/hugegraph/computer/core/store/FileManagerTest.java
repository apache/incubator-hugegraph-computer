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

package org.apache.hugegraph.computer.core.store;

import java.io.File;
import java.io.IOException;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class FileManagerTest extends UnitTestBase {

    @Test
    public void testInitWithFile() throws IOException {
        File file = new File("exist");
        file.createNewFile();
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.WORKER_DATA_DIRS, "[" + file.getAbsolutePath() + "]"
        );
        FileManager dataFileManager = new FileManager();
        Assert.assertEquals(FileManager.NAME, dataFileManager.name());
        Assert.assertThrows(ComputerException.class, () -> {
            dataFileManager.init(config);
        }, e -> {
            Assert.assertContains("Can't create dir ", e.getMessage());
        });
        file.delete();
    }

    @Test
    public void testInitWithReadOnlyDir() {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.WORKER_DATA_DIRS, "[/etc]"
        );
        FileManager dataFileManager = new FileManager();
        Assert.assertThrows(ComputerException.class, () -> {
            dataFileManager.init(config);
        }, e -> {
            Assert.assertContains("Can't create dir", e.getMessage());
        });
    }

    @Test
    public void testNextDir() {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]"
        );
        FileManager dataFileManager = new FileManager();

        dataFileManager.init(config);

        String dir1 = dataFileManager.nextDirectory();
        String dir2 = dataFileManager.nextDirectory();
        String dir3 = dataFileManager.nextDirectory();
        String dir4 = dataFileManager.nextDirectory();
        String dir5 = dataFileManager.nextDirectory();
        Assert.assertEquals(dir1, dir3);
        Assert.assertEquals(dir3, dir5);
        Assert.assertEquals(dir2, dir4);

        dataFileManager.close(config);
    }

    @Test
    public void testNextDirWithPaths() {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]"
        );
        FileManager dataFileManager = new FileManager();

        dataFileManager.init(config);

        File dir1 = new File(dataFileManager.nextDirectory("vertex"));
        Assert.assertEquals("vertex", dir1.getName());

        File dir2 = new File(dataFileManager.nextDirectory("message", "1"));
        Assert.assertEquals("1", dir2.getName());
        Assert.assertEquals("message", dir2.getParentFile().getName());

        dataFileManager.close(config);
    }
}
