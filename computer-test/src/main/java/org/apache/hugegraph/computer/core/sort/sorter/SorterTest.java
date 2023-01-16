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

package org.apache.hugegraph.computer.core.sort.sorter;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.combiner.IntValueSumCombiner;
import org.apache.hugegraph.computer.core.combiner.PointerCombiner;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.io.BytesInput;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.sort.Sorter;
import org.apache.hugegraph.computer.core.sort.SorterTestUtil;
import org.apache.hugegraph.computer.core.sort.flusher.CombineKvInnerSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.CombineKvOuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.CombineSubKvInnerSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.CombineSubKvOuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.KvEntryFileReader;
import org.apache.hugegraph.computer.core.store.StoreTestUtil;
import org.apache.hugegraph.computer.core.store.buffer.KvEntriesInput;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDir;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDirImpl;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.reader.HgkvDirReaderImpl;
import org.apache.hugegraph.computer.core.store.file.select.DisperseEvenlySelector;
import org.apache.hugegraph.computer.core.store.file.select.InputFilesSelector;
import org.apache.hugegraph.computer.core.util.FileUtil;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SorterTest {

    @Before
    public void setup() throws IOException {
        FileUtils.deleteDirectory(new File(StoreTestUtil.FILE_DIR));
    }

    @After
    public void teardown() throws IOException {
        FileUtils.deleteDirectory(new File(StoreTestUtil.FILE_DIR));
    }

    @Test
    public void testSortKvBuffer() throws Exception {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MAX_FILE_SIZE, "32",
                ComputerOptions.HGKV_DATABLOCK_SIZE, "16",
                ComputerOptions.HGKV_MERGE_FILES_NUM, "3"
        );
        List<Integer> map = ImmutableList.of(2, 3,
                                             1, 23,
                                             6, 2,
                                             5, 9,
                                             2, 2,
                                             6, 1,
                                             1, 20);
        BytesInput input = SorterTestUtil.inputFromKvMap(map);
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);

        Sorter sorter = SorterTestUtil.createSorter(config);
        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                                                  IntValue::new,
                                                  new IntValueSumCombiner());
        sorter.sortBuffer(input,
                          new CombineKvInnerSortFlusher(output, combiner),
                          false);

        BytesInput resultInput = EntriesUtil.inputFromOutput(output);
        KvEntriesInput iter = new KvEntriesInput(resultInput);
        SorterTestUtil.assertKvEntry(iter.next(), 1, 43);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 5);
        SorterTestUtil.assertKvEntry(iter.next(), 5, 9);
        SorterTestUtil.assertKvEntry(iter.next(), 6, 3);
        iter.close();
    }

    @Test
    public void testSortKvBuffers() throws Exception {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MAX_FILE_SIZE, "32",
                ComputerOptions.HGKV_DATABLOCK_SIZE, "16",
                ComputerOptions.HGKV_MERGE_FILES_NUM, "3",
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
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
        Sorter sorter = SorterTestUtil.createSorter(config);
        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                                                  IntValue::new,
                                                  new IntValueSumCombiner());
        sorter.mergeBuffers(inputs, new CombineKvOuterSortFlusher(combiner),
                            path, false);

        // Assert merge result from target hgkvDir
        KvEntryFileReader reader = new HgkvDirReaderImpl(path, false);
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
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MAX_FILE_SIZE, "32",
                ComputerOptions.HGKV_DATABLOCK_SIZE, "16",
                ComputerOptions.HGKV_MERGE_FILES_NUM, "3",
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
        this.testMergeKvInputs(config);

        config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MAX_FILE_SIZE, "32",
                ComputerOptions.HGKV_DATABLOCK_SIZE, "16",
                ComputerOptions.HGKV_MERGE_FILES_NUM, "3",
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "true"
        );
        this.testMergeKvInputs(config);
    }

    private void testMergeKvInputs(Config config) throws Exception {
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
            if (config.get(ComputerOptions.TRANSPORT_RECV_FILE_MODE)) {
                StoreTestUtil.bufferFileFromKvMap(map, inputs.get(i));
            } else {
                StoreTestUtil.hgkvDirFromKvMap(config, map, inputs.get(i));
            }
        }

        // Merge file
        Sorter sorter = SorterTestUtil.createSorter(config);
        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                                                  IntValue::new,
                                                  new IntValueSumCombiner());
        sorter.mergeInputs(inputs, new CombineKvOuterSortFlusher(combiner),
                           outputs, false);

        // Assert sort result
        List<Integer> result = ImmutableList.of(1, 25,
                                                2, 20,
                                                3, 10,
                                                5, 10,
                                                6, 55);
        Iterator<Integer> resultIter = result.iterator();
        Iterator<KvEntry> iterator = sorter.iterator(outputs, false);
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

        FileUtil.deleteFilesQuietly(inputs);
        FileUtil.deleteFilesQuietly(outputs);
    }

    private BytesInput sortedSubKvBuffer(Config config) throws Exception {
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

        BytesInput input = SorterTestUtil.inputFromSubKvMap(data);
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                                                  IntValue::new,
                                                  new IntValueSumCombiner());
        int flushThreshold = config.get(
                             ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX);
        InnerSortFlusher flusher = new CombineSubKvInnerSortFlusher(
                                       output, combiner, flushThreshold);

        Sorter sorter = SorterTestUtil.createSorter(config);
        sorter.sortBuffer(input, flusher, true);

        return EntriesUtil.inputFromOutput(output);
    }

    @Test
    public void testSortSubKvBuffer() throws Exception {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX, "2"
        );

        /*
         * Assert result
         * key 1 subKv 3 1, 5 1
         * key 2 subKv 5 1, 8 2
         * key 2 subKv 9 1
         * key 3 subKv 2 2, 3 1
         * key 3 subKv 4 1
         */
        BytesInput input = this.sortedSubKvBuffer(config);
        EntryIterator iter = new KvEntriesInput(input, true);
        SorterTestUtil.assertSubKvByKv(iter.next(), 1, 3, 1, 5, 1);
        SorterTestUtil.assertSubKvByKv(iter.next(), 2, 5, 1, 8, 2);
        SorterTestUtil.assertSubKvByKv(iter.next(), 2, 9, 1);
        SorterTestUtil.assertSubKvByKv(iter.next(), 3, 2, 2, 3, 1);
        SorterTestUtil.assertSubKvByKv(iter.next(), 3, 4, 1);
        iter.close();
    }

    @Test
    public void testSortSubKvBuffers() throws Exception {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX, "2",
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
        int flushThreshold = config.get(
                             ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX);

        BytesInput i1 = this.sortedSubKvBuffer(config);
        BytesInput i2 = this.sortedSubKvBuffer(config);
        BytesInput i3 = this.sortedSubKvBuffer(config);
        List<RandomAccessInput> buffers = ImmutableList.of(i1, i2, i3);

        Sorter sorter = SorterTestUtil.createSorter(config);
        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                                                  IntValue::new,
                                                  new IntValueSumCombiner());
        OuterSortFlusher flusher = new CombineSubKvOuterSortFlusher(
                                       combiner, flushThreshold);
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
        Iterator<KvEntry> kvIter = sorter.iterator(outputs, true);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 1, 3, 3, 5, 3);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 2, 5, 3, 8, 6);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 2, 9, 3);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 3, 2, 6, 3, 3);
        SorterTestUtil.assertSubKvByKv(kvIter.next(), 3, 4, 3);

        // Assert file properties
        HgkvDir dir = HgkvDirImpl.open(outputFile);
        Assert.assertEquals(5, dir.numEntries());
        Assert.assertEquals(8, dir.numSubEntries());
    }

    @Test
    public void testMergeSubKvFiles() throws Exception {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX, "2",
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
        this.testMergeSubKvFiles(config);

        config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX, "2",
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "true"
        );
        this.testMergeSubKvFiles(config);
    }

    private void testMergeSubKvFiles(Config config) throws Exception {
        int flushThreshold = config.get(
                ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX);

        List<Integer> kv1 = ImmutableList.of(1,
                                             2, 1,
                                             4, 1);
        List<Integer> kv2 = ImmutableList.of(4,
                                             2, 1,
                                             3, 1);
        List<Integer> kv3 = ImmutableList.of(4,
                                             6, 1,
                                             8, 1);
        List<Integer> kv4 = ImmutableList.of(1,
                                             1, 1,
                                             2, 1);
        List<Integer> kv5 = ImmutableList.of(1,
                                             5, 1,
                                             7, 1);
        List<Integer> kv6 = ImmutableList.of(2,
                                             2, 1,
                                             5, 1);

        List<List<Integer>> data1 = ImmutableList.of(kv1, kv2, kv3);
        List<List<Integer>> data2 = ImmutableList.of(kv4, kv5, kv6);
        List<List<Integer>> data3 = ImmutableList.of(kv4, kv1, kv3);
        List<List<List<Integer>>> datas = ImmutableList.of(data1, data2, data3);

        String input1 = StoreTestUtil.availablePathById(1);
        String input2 = StoreTestUtil.availablePathById(2);
        String input3 = StoreTestUtil.availablePathById(3);
        String output = StoreTestUtil.availablePathById(0);

        List<String> inputs = ImmutableList.of(input1, input2, input3);
        List<String> outputs = ImmutableList.of(output);

        boolean useBufferFile = config.get(
                ComputerOptions.TRANSPORT_RECV_FILE_MODE);
        for (int i = 0; i < inputs.size(); i++) {
            String input = inputs.get(i);
            List<List<Integer>> data = datas.get(i);
            if (useBufferFile) {
                StoreTestUtil.bufferFileFromSubKvMap(data, input);
            } else {
                StoreTestUtil.hgkvDirFromSubKvMap(config, data, input);
            }
        }

        Sorter sorter = SorterTestUtil.createSorter(config);
        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                IntValue::new,
                new IntValueSumCombiner());
        OuterSortFlusher flusher = new CombineSubKvOuterSortFlusher(
                combiner, flushThreshold);
        flusher.sources(inputs.size());
        sorter.mergeInputs(inputs, flusher, outputs, true);

        /* Assert result
         * key 1 subKv 1 2 2 4
         * key 1 subKv 4 2 5 1
         * key 1 subKv 7 1
         * key 2 subKv 2 1 5 1
         * key 4 subKv 2 1 3 1
         * key 4 subKv 6 2 8 2
         */
        try (CIter<KvEntry> kvIter = sorter.iterator(outputs, true)) {
            SorterTestUtil.assertSubKvByKv(kvIter.next(), 1, 1, 2, 2, 4);
            SorterTestUtil.assertSubKvByKv(kvIter.next(), 1, 4, 2, 5, 1);
            SorterTestUtil.assertSubKvByKv(kvIter.next(), 1, 7, 1);
            SorterTestUtil.assertSubKvByKv(kvIter.next(), 2, 2, 1, 5, 1);
            SorterTestUtil.assertSubKvByKv(kvIter.next(), 4, 2, 1, 3, 1);
            SorterTestUtil.assertSubKvByKv(kvIter.next(), 4, 6, 2, 8, 2);
        }

        FileUtil.deleteFilesQuietly(inputs);
        FileUtil.deleteFilesQuietly(outputs);
    }

    @Test
    public void testExceptionCaseForSelector() {
        // Parameter inputs size < outputs size
        String input1 = StoreTestUtil.availablePathById("1");
        String input2 = StoreTestUtil.availablePathById("2");
        List<String> inputs = ImmutableList.of(input1, input2);

        String output1 = StoreTestUtil.availablePathById("3");
        String output2 = StoreTestUtil.availablePathById("4");
        String output3 = StoreTestUtil.availablePathById("5");
        List<String> outputs = ImmutableList.of(output1, output2, output3);

        InputFilesSelector selector = new DisperseEvenlySelector();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            selector.selectedByHgkvFile(inputs, outputs);
        }, e -> {
            String errorMsg = "inputs size of InputFilesSelector must be >= " +
                              "outputs size";
            Assert.assertContains(errorMsg, e.getMessage());
        });
    }
}
