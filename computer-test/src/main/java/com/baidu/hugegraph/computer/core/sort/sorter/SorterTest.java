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

package com.baidu.hugegraph.computer.core.sort.sorter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayInput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayOutput;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.SorterTestUtil;
import com.baidu.hugegraph.computer.core.sort.flusher.MockInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.MockOutSortFlusher;
import com.baidu.hugegraph.computer.core.store.iter.CloseableIterator;
import com.baidu.hugegraph.computer.core.store.StoreTestUtil;
import com.baidu.hugegraph.computer.core.store.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.file.reader.HgkvDirReader;
import com.baidu.hugegraph.computer.core.store.file.reader.HgkvDirReaderImpl;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class SorterTest {

    private static Config CONFIG;

    @BeforeClass
    public static void init() {
        OptionSpace.register("computer",
                             "com.baidu.hugegraph.computer.core.config." +
                             "ComputerOptions");
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MAX_FILE_SIZE, "32",
                ComputerOptions.HGKV_DATABLOCK_SIZE, "16",
                ComputerOptions.HGKV_MERGE_PATH_NUM, "3"
        );
        CONFIG = ComputerContext.instance().config();
    }

    @After
    public void teardown() {
        FileUtils.deleteQuietly(new File(StoreTestUtil.FILE_DIR));
    }

    @Test
    public void testSortBuffer() throws IOException {
        List<Integer> map = ImmutableList.of(2, 3,
                                             1, 23,
                                             6, 2,
                                             5, 9,
                                             2, 2,
                                             6, 1,
                                             1, 20);
        UnsafeByteArrayOutput data = SorterTestUtil.writeMapToOutput(map);

        RandomAccessInput input = new UnsafeByteArrayInput(data.buffer(),
                                                           data.position());

        SorterImpl sorter = new SorterImpl(CONFIG);
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        sorter.sortBuffer(input, new MockInnerSortFlusher(output));

        Map<Integer, List<Integer>> result = ImmutableMap.of(
                1, ImmutableList.of(23, 20),
                2, ImmutableList.of(3, 2),
                5, ImmutableList.of(9),
                6, ImmutableList.of(2, 1));
        SorterTestUtil.assertOutputEqualsMap(output, result);
    }

    @Test
    public void testSortBuffers() throws IOException {
        List<Integer> map1 = ImmutableList.of(2, 3,
                                              2, 1,
                                              5, 2,
                                              6, 9,
                                              6, 2);
        List<Integer> map2 = ImmutableList.of(1, 3,
                                              1, 1,
                                              3, 2,
                                              6, 9,
                                              8, 2);
        String path = StoreTestUtil.availablePathById("1");
        File file = new File(path);
        CloseableIterator<KvEntry> iterator = null;
        try {
            // Merge 4 sorted input
            List<RandomAccessInput> inputs = ImmutableList.of(
                                    SorterTestUtil.inputFromMap(map1),
                                    SorterTestUtil.inputFromMap(map2),
                                    SorterTestUtil.inputFromMap(map1),
                                    SorterTestUtil.inputFromMap(map2));
            SorterImpl sorter = new SorterImpl(CONFIG);
            sorter.mergeBuffers(inputs, new MockOutSortFlusher(), path, false);

            // Assert merge result from target hgkvDir
            HgkvDirReader reader = new HgkvDirReaderImpl(path);
            iterator = reader.iterator();
            SorterTestUtil.assertKvEntry(iterator.next(), 1, 8);
            SorterTestUtil.assertKvEntry(iterator.next(), 2, 8);
            SorterTestUtil.assertKvEntry(iterator.next(), 3, 4);
            SorterTestUtil.assertKvEntry(iterator.next(), 5, 4);
            SorterTestUtil.assertKvEntry(iterator.next(), 6, 40);
            SorterTestUtil.assertKvEntry(iterator.next(), 8, 4);
            Assert.assertFalse(iterator.hasNext());
        } finally {
            FileUtils.deleteQuietly(file);
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Test
    public void testMergeInputs() throws IOException {
        List<Integer> map1 = ImmutableList.of(2, 3,
                                              2, 1,
                                              5, 2,
                                              6, 9,
                                              6, 2);
        List<Integer> map2 = ImmutableList.of(1, 3,
                                              1, 2,
                                              3, 2);

        // Input hgkvDirs
        String file1Name = StoreTestUtil.availablePathById("1");
        String file2Name = StoreTestUtil.availablePathById("2");
        String file3Name = StoreTestUtil.availablePathById("3");
        String file4Name = StoreTestUtil.availablePathById("4");
        String file5Name = StoreTestUtil.availablePathById("5");
        String file6Name = StoreTestUtil.availablePathById("6");
        String file7Name = StoreTestUtil.availablePathById("7");
        String file8Name = StoreTestUtil.availablePathById("8");
        String file9Name = StoreTestUtil.availablePathById("9");
        String file10Name = StoreTestUtil.availablePathById("10");
        List<String> inputs = Lists.newArrayList(file1Name, file2Name,
                                                 file3Name, file4Name,
                                                 file5Name, file6Name,
                                                 file7Name, file8Name,
                                                 file9Name, file10Name);
        // Output hgkvDirs
        String output1 = StoreTestUtil.availablePathById("20");
        String output2 = StoreTestUtil.availablePathById("21");
        List<String> outputs = ImmutableList.of(output1, output2);

        List<File> inputFiles = new ArrayList<>();
        List<File> outputFiles = Lists.newArrayList(new File(output1),
                                                    new File(output2));
        try {
            for (int i = 0; i < inputs.size(); i++) {
                List<Integer> map;
                if ((i & 1) == 0) {
                    map = map1;
                } else {
                    map = map2;
                }
                File file = StoreTestUtil.hgkvDirFromMap(map, inputs.get(i));
                inputFiles.add(file);
            }

            // Merge file
            Sorter sorter = new SorterImpl(CONFIG);
            sorter.mergeInputs(inputs, new MockOutSortFlusher(), outputs,
                               false);

            // Assert sort result
            List<Integer> result = ImmutableList.of(1, 25,
                                                    2, 20,
                                                    3, 10,
                                                    5, 10,
                                                    6, 55);
            Iterator<Integer> resultIter = result.iterator();
            CloseableIterator<KvEntry> iterator = sorter.iterator(outputs);
            KvEntry last = iterator.next();
            int value = StoreTestUtil.dataFromPointer(last.value());
            while (true) {
                KvEntry current = null;
                if (iterator.hasNext()) {
                    current = iterator.next();
                    if (last.compareTo(current) == 0) {
                        value += StoreTestUtil.dataFromPointer(current.value());
                        continue;
                    }
                }

                Assert.assertEquals(StoreTestUtil.dataFromPointer(last.key()),
                                    resultIter.next());
                Assert.assertEquals(value, resultIter.next());

                if (current == null) {
                    break;
                }

                last = current;
                value = StoreTestUtil.dataFromPointer(last.value());
            }
            Assert.assertFalse(resultIter.hasNext());
        } finally {
            inputFiles.forEach(FileUtils::deleteQuietly);
            outputFiles.forEach(FileUtils::deleteQuietly);
        }
    }
}
