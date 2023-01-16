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
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDir;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDirImpl;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvFileImpl;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.builder.HgkvDirBuilderImpl;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.reader.HgkvDirReaderImpl;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class HgkvDirTest {

    private static Config CONFIG;

    @BeforeClass
    public static void init() {
        CONFIG = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MAX_FILE_SIZE, "32",
                ComputerOptions.HGKV_DATABLOCK_SIZE, "16"
        );
    }

    @Before
    public void setup() throws IOException {
        FileUtils.deleteDirectory(new File(StoreTestUtil.FILE_DIR));
    }

    @After
    public void teardown() throws IOException {
        FileUtils.deleteDirectory(new File(StoreTestUtil.FILE_DIR));
    }

    @Test
    public void testHgkvDirBuilder() throws IOException {
        // The data must be ordered
        List<Integer> data = ImmutableList.of(2, 3,
                                              2, 1,
                                              5, 2,
                                              5, 9,
                                              6, 2);
        List<KvEntry> kvEntries = StoreTestUtil.kvEntriesFromMap(data);

        String path = StoreTestUtil.availablePathById("1");
        try (KvEntryFileWriter builder = new HgkvDirBuilderImpl(CONFIG, path)) {
            for (KvEntry entry : kvEntries) {
                builder.write(entry);
            }
            builder.finish();

            // Open the file and determine the footer is as expected
            HgkvDir dir = HgkvDirImpl.open(path);
            Assert.assertEquals(HgkvFileImpl.MAGIC, dir.magic());
            String version = HgkvDirImpl.MAJOR_VERSION + "." +
                             HgkvDirImpl.MINOR_VERSION;
            Assert.assertEquals(version, dir.version());
            Assert.assertEquals(5, dir.numEntries());
            int maxKey = StoreTestUtil.byteArrayToInt(dir.max());
            Assert.assertEquals(6, maxKey);
            int minKey = StoreTestUtil.byteArrayToInt(dir.min());
            Assert.assertEquals(2, minKey);
        } finally {
            FileUtils.deleteQuietly(new File(path));
        }
    }

    @Test
    public void testHgkvDirReader() throws Exception {
        // The keys in the data must be ordered
        List<Integer> data = ImmutableList.of(2, 3,
                                              2, 1,
                                              5, 2,
                                              5, 5,
                                              5, 9,
                                              6, 2);
        String path = StoreTestUtil.availablePathById("1");
        StoreTestUtil.hgkvDirFromKvMap(CONFIG, data, path);
        KvEntryFileReader reader = new HgkvDirReaderImpl(path, false);
        try (EntryIterator iterator = reader.iterator()) {
            int i = 0;
            while (iterator.hasNext()) {
                KvEntry entry = iterator.next();
                int key = StoreTestUtil.byteArrayToInt(entry.key().bytes());
                Assert.assertEquals(data.get(i).intValue(), key);
                i += 2;
            }
            Assert.assertThrows(NoSuchElementException.class, iterator::next);
        }
    }

    @Test
    public void testExceptionCaseForHgkvDir() throws IOException {
        // Path isn't directory
        File file = new File(StoreTestUtil.availablePathById("1"));
        file.getParentFile().mkdirs();
        file.createNewFile();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            HgkvDirImpl.open(file.getPath());
        }, e -> {
            Assert.assertContains("because it's not a directory",
                                  e.getMessage());
        });
        FileUtils.deleteQuietly(file);

        // Open not exists file
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            HgkvDirImpl.open(file.getPath());
        }, e -> {
            Assert.assertContains("because it does not exists",
                                  e.getMessage());
        });
    }
}
