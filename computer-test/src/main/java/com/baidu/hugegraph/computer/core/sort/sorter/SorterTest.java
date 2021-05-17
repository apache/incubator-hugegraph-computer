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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.SorterTestUtil;
import com.baidu.hugegraph.computer.core.sort.combiner.MockIntSumCombiner;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.KvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.KvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineSubKvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineSubKvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.store.StoreTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntriesInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReader;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReaderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SorterTest {

    private static Config CONFIG;

    @BeforeClass
    public static void init() {
        CONFIG = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MAX_FILE_SIZE, "32",
                ComputerOptions.HGKV_DATABLOCK_SIZE, "16",
                ComputerOptions.HGKV_MERGE_PATH_NUM, "3"
        );
    }

    @After
    public void teardown() {
        FileUtils.deleteQuietly(new File(StoreTestUtil.FILE_DIR));
    }

    @Test
    public void testSortKvBuffer() throws IOException {
        List<Integer> map = ImmutableList.of(2, 3,
                                             1, 23,
                                             6, 2,
                                             5, 9,
                                             2, 2,
                                             6, 1,
                                             1, 20);
        UnsafeBytesInput input = SorterTestUtil.inputFromKvMap(map);
        UnsafeBytesOutput output = new UnsafeBytesOutput();

        SorterImpl sorter = new SorterImpl(CONFIG);
        Combiner<Pointer> combiner = new MockIntSumCombiner();
        sorter.sortBuffer(input, new CombineKvInnerSortFlusher(output,
                                                               combiner));

        UnsafeBytesInput resultInput = EntriesUtil.inputFromOutput(output);
        Iterator<KvEntry> iter = new EntriesInput(resultInput);
        SorterTestUtil.assertKvEntry(iter.next(), 1, 43);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 5);
        SorterTestUtil.assertKvEntry(iter.next(), 5, 9);
        SorterTestUtil.assertKvEntry(iter.next(), 6, 3);
    }

    @Test
    public void testSortKvBuffers() throws Exception {
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

        // Merge 4 sorted input
        List<RandomAccessInput> inputs = ImmutableList.of(
                                SorterTestUtil.inputFromKvMap(map1),
                                SorterTestUtil.inputFromKvMap(map2),
                                SorterTestUtil.inputFromKvMap(map1),
                                SorterTestUtil.inputFromKvMap(map2));
        SorterImpl sorter = new SorterImpl(CONFIG);
        Combiner<Pointer> combiner = new MockIntSumCombiner();
        sorter.mergeBuffers(inputs, new CombineKvOuterSortFlusher(combiner),
                            path, false);

        // Assert merge result from target hgkvDir
        HgkvDirReader reader = new HgkvDirReaderImpl(path, false);
        EntryIterator iter = reader.iterator();
        SorterTestUtil.assertKvEntry(iter.next(), 1, 8);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 8);
        SorterTestUtil.assertKvEntry(iter.next(), 3, 4);
        SorterTestUtil.assertKvEntry(iter.next(), 5, 4);
        SorterTestUtil.assertKvEntry(iter.next(), 6, 40);
        SorterTestUtil.assertKvEntry(iter.next(), 8, 4);
        Assert.assertFalse(iter.hasNext());
    }

    @Test
    public void testMergeKvInputs() throws Exception {
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

        for (int i = 0; i < inputs.size(); i++) {
            List<Integer> map;
            if ((i & 1) == 0) {
                map = map1;
            } else {
                map = map2;
            }
            StoreTestUtil.hgkvDirFromMap(map, inputs.get(i));
        }

        // Merge file
        Sorter sorter = new SorterImpl(CONFIG);
        Combiner<Pointer> combiner = new MockIntSumCombiner();
        sorter.mergeInputs(inputs, new CombineKvOuterSortFlusher(combiner),
                           outputs, false);

        // Assert sort result
        List<Integer> result = ImmutableList.of(1, 25,
                                                2, 20,
                                                3, 10,
                                                5, 10,
                                                6, 55);
        Iterator<Integer> resultIter = result.iterator();
        Iterator<KvEntry> iterator = sorter.iterator(outputs);
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
    }

    private UnsafeBytesInput sortedSubKvBuffer(Config config)
                                               throws IOException {
        List<Integer> kv1 = ImmutableList.of(3,
                                             2, 1,
                                             4, 1);
        List<Integer> kv2 = ImmutableList.of(1,
                                             3, 1,
                                             5, 1);
        List<Integer> kv3 = ImmutableList.of(2,
                                             8, 1,
                                             9, 1);
        List<Integer> kv4 = ImmutableList.of(3,
                                             2, 1,
                                             3, 1);
        List<Integer> kv5 = ImmutableList.of(2,
                                             5, 1,
                                             8, 1);
        List<List<Integer>> data = ImmutableList.of(kv1, kv2, kv3, kv4, kv5);

        UnsafeBytesInput input = SorterTestUtil.inputFromSubKvMap(data);
        UnsafeBytesOutput output = new UnsafeBytesOutput();
        Combiner<Pointer> combiner = new MockIntSumCombiner();
        int subKvFlushThreshold = config.get(
                                  ComputerOptions.OUTPUT_EDGE_BATCH_SIZE);
        InnerSortFlusher flusher = new CombineSubKvInnerSortFlusher(
                                       output, combiner, subKvFlushThreshold);

        Sorter sorter = new SorterImpl(config);
        sorter.sortBuffer(input, flusher);

        return EntriesUtil.inputFromOutput(output);
    }

    @Test
    public void testSortSubKvBuffer() throws IOException {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.OUTPUT_EDGE_BATCH_SIZE, "2"
        );

        /*
         * Assert result
         * key 1 subKv 3 1, 5 1
         * key 2 subKv 5 1, 8 2
         * key 2 subKv 9 1
         * key 3 subKv 2 2, 3 1
         * key 3 subKv 4 1
         */
        EntryIterator kvIter = new EntriesInput(this.sortedSubKvBuffer(config));
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 1, 3, 1, 5, 1);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 2, 5, 1, 8, 2);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 2, 9, 1);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 3, 2, 2, 3, 1);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 3, 4, 1);
    }

    @Test
    public void testSortSubKvBuffers() throws IOException {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.OUTPUT_EDGE_BATCH_SIZE, "2"
        );
        Integer subKvFlushThreshold = config.get(
                                      ComputerOptions.OUTPUT_EDGE_BATCH_SIZE);

        UnsafeBytesInput i1 = this.sortedSubKvBuffer(config);
        UnsafeBytesInput i2 = this.sortedSubKvBuffer(config);
        UnsafeBytesInput i3 = this.sortedSubKvBuffer(config);
        List<RandomAccessInput> buffers = ImmutableList.of(i1, i2, i3);

        Sorter sorter = new SorterImpl(config);
        Combiner<Pointer> combiner = new MockIntSumCombiner();
        OuterSortFlusher flusher = new CombineSubKvOuterSortFlusher(
                                   combiner, subKvFlushThreshold);
        flusher.sources(buffers.size());

        String outputFile = StoreTestUtil.availablePathById("1");
        sorter.mergeBuffers(buffers, flusher, outputFile, true);

        /*
         * Assert result
         * key 1 subKv 3 3, 5 3
         * key 2 subKv 5 3, 8 6
         * key 2 subKv 9 3
         * key 3 subKv 2 6, 3 3
         * key 3 subKv 4 3
         */
        ImmutableList<String> outputs = ImmutableList.of(outputFile);
        Iterator<KvEntry> kvIter = sorter.iterator(outputs);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 1, 3, 3, 5, 3);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 2, 5, 3, 8, 6);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 2, 9, 3);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 3, 2, 6, 3, 3);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 3, 4, 3);
    }

    @Test
    public void testKvInnerSortFlusher() throws IOException {
        List<Integer> map = ImmutableList.of(2, 1,
                                             3, 1,
                                             2, 1,
                                             4, 1);
        UnsafeBytesInput input = SorterTestUtil.inputFromKvMap(map);

        UnsafeBytesOutput output = new UnsafeBytesOutput();
        Sorter sorter = new SorterImpl(CONFIG);
        sorter.sortBuffer(input, new KvInnerSortFlusher(output));

        UnsafeBytesInput result = EntriesUtil.inputFromOutput(output);
        EntryIterator iter = new EntriesInput(result);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 3, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 4, 1);
    }

    @Test
    public void testKvOuterSortFlusher() throws IOException {
        List<Integer> map1 = ImmutableList.of(2, 1,
                                              2, 1,
                                              3, 1,
                                              4, 1);
        List<Integer> map2 = ImmutableList.of(1, 1,
                                              3, 1,
                                              6, 1);
        UnsafeBytesInput input1 = SorterTestUtil.inputFromKvMap(map1);
        UnsafeBytesInput input2 = SorterTestUtil.inputFromKvMap(map2);
        List<RandomAccessInput> inputs = ImmutableList.of(input1, input2);

        String resultFile = StoreTestUtil.availablePathById("1");
        Sorter sorter = new SorterImpl(CONFIG);
        sorter.mergeBuffers(inputs, new KvOuterSortFlusher(), resultFile,
                            false);

        ImmutableList<String> outputs = ImmutableList.of(resultFile);
        Iterator<KvEntry> iter = sorter.iterator(outputs);
        SorterTestUtil.assertKvEntry(iter.next(), 1, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 3, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 3, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 4, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 6, 1);
    }
}
