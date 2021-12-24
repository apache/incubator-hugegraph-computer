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
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.combiner.IntValueSumCombiner;
import com.baidu.hugegraph.computer.core.combiner.PointerCombiner;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.sort.SorterTestUtil;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.KvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.StoreTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.DefaultKvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.InlinePointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDir;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDirImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilderImpl;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SortLargeDataTest {

    private static final Logger LOG = Log.logger(SortLargeDataTest.class);
    private static Config CONFIG;

    @BeforeClass
    public static void init() {
        CONFIG = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MERGE_FILES_NUM, "200",
                ComputerOptions.HGKV_MAX_FILE_SIZE, String.valueOf(Bytes.GB)
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
    public void testAllProcess() throws Exception {
        StopWatch watcher = new StopWatch();
        final long bufferSize = Bytes.MB;
        final int mergeBufferNum = 300;
        final int dataSize = 1000000;
        long value = 0;

        Random random = new Random();
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        List<RandomAccessInput> buffers = new ArrayList<>(mergeBufferNum);
        List<String> mergeBufferFiles = new ArrayList<>();
        int fileNum = 10;
        Sorter sorter = new SorterImpl(CONFIG);

        watcher.start();
        for (int i = 0; i < dataSize; i++) {
            SorterTestUtil.writeData(output, random.nextInt(dataSize));
            int entryValue = random.nextInt(5);
            SorterTestUtil.writeData(output, entryValue);
            value = value + entryValue;

            // Write data to buffer and sort buffer
            if (output.position() >= bufferSize || (i + 1) == dataSize) {
                BytesInput input = EntriesUtil.inputFromOutput(output);
                buffers.add(sortBuffer(sorter, input));
                output.seek(0);
            }

            // Merge buffers to HgkvDir
            if (buffers.size() >= mergeBufferNum || (i + 1) == dataSize) {
                String outputFile = StoreTestUtil.availablePathById(fileNum++);
                mergeBufferFiles.add(outputFile);
                mergeBuffers(sorter, buffers, outputFile);
                buffers.clear();
            }
        }

        // Merge file
        String resultFile = StoreTestUtil.availablePathById("0");
        mergeFiles(sorter, mergeBufferFiles, Lists.newArrayList(resultFile));

        watcher.stop();
        LOG.info("testAllProcess sort time: {}", watcher.getTime());

        long result = sumOfEntryValue(sorter, ImmutableList.of(resultFile));
        Assert.assertEquals(value, result);
    }

    @Test
    public void testMergeBuffers() throws Exception {
        StopWatch watcher = new StopWatch();
        // Sort buffers total size 100M, each buffer is 50KB
        final long bufferSize = Bytes.KB * 50;
        final long bufferNum = 2000;
        final int keyRange = 10000000;
        long totalValue = 0L;

        Random random = new Random();
        List<RandomAccessInput> buffers = new ArrayList<>();
        for (int i = 0; i < bufferNum; i++) {
            BytesOutput buffer = IOFactory.createBytesOutput(
                                 Constants.SMALL_BUF_SIZE);
            while (buffer.position() < bufferSize) {
                // Write data
                int key = random.nextInt(keyRange);
                SorterTestUtil.writeData(buffer, key);
                int value = random.nextInt(100);
                SorterTestUtil.writeData(buffer, value);
                totalValue += value;
            }
            buffers.add(EntriesUtil.inputFromOutput(buffer));
        }

        // Sort buffer
        Sorter sorter = new SorterImpl(CONFIG);
        watcher.start();
        List<RandomAccessInput> sortedBuffers = new ArrayList<>();
        for (RandomAccessInput buffer : buffers) {
            RandomAccessInput sortedBuffer = sortBuffer(sorter, buffer);
            sortedBuffers.add(sortedBuffer);
        }
        watcher.stop();

        LOG.info("testMergeBuffers sort buffer cost time: {}",
                 watcher.getTime());

        String resultFile = StoreTestUtil.availablePathById("0");
        // Sort buffers
        watcher.reset();
        watcher.start();
        sorter.mergeBuffers(sortedBuffers, new KvOuterSortFlusher(),
                            resultFile, false);
        watcher.stop();

        LOG.info("testMergeBuffers merge buffers cost time: {}",
                 watcher.getTime());

        // Assert result
        long result = sumOfEntryValue(sorter, ImmutableList.of(resultFile));
        Assert.assertEquals(totalValue, result);
        assertFileOrder(sorter, ImmutableList.of(resultFile));
    }

    @Test
    public void testMergeBuffersAllSameKey() throws Exception {
        List<RandomAccessInput> buffers = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            BytesOutput buffer = IOFactory.createBytesOutput(
                                 Constants.SMALL_BUF_SIZE);
            for (int j = 0; j < 100; j++) {
                // Write data
                SorterTestUtil.writeData(buffer, 1);
                SorterTestUtil.writeData(buffer, 1);
            }
            buffers.add(EntriesUtil.inputFromOutput(buffer));
        }

        String resultFile = StoreTestUtil.availablePathById("0");
        Sorter sorter = new SorterImpl(CONFIG);
        mergeBuffers(sorter, buffers, resultFile);

        // Assert result
        long result = sumOfEntryValue(sorter, ImmutableList.of(resultFile));
        Assert.assertEquals(1000 * 100, result);
    }

    @Test
    public void testDiffNumEntriesFileMerge() throws Exception {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MERGE_FILES_NUM, "3"
        );
        List<Integer> sizeList = ImmutableList.of(200, 500, 20, 50, 300,
                                                  250, 10, 33, 900, 89, 20);
        List<String> inputs = new ArrayList<>();

        for (int j = 0; j < sizeList.size(); j++) {
            String file = StoreTestUtil.availablePathById(j + 10);
            inputs.add(file);
            try (HgkvDirBuilder builder = new HgkvDirBuilderImpl(config,
                                                                 file)) {
                for (int i = 0; i < sizeList.get(j); i++) {
                    byte[] keyBytes = StoreTestUtil.intToByteArray(i);
                    byte[] valueBytes = StoreTestUtil.intToByteArray(1);
                    Pointer key = new InlinePointer(keyBytes);
                    Pointer value = new InlinePointer(valueBytes);
                    KvEntry entry = new DefaultKvEntry(key, value);
                    builder.write(entry);
                }
            }
        }

        List<String> outputs = ImmutableList.of(
                               StoreTestUtil.availablePathById(0),
                               StoreTestUtil.availablePathById(1),
                               StoreTestUtil.availablePathById(2),
                               StoreTestUtil.availablePathById(3));
        Sorter sorter = new SorterImpl(config);
        sorter.mergeInputs(inputs, new KvOuterSortFlusher(), outputs, false);

        int total = sizeList.stream().mapToInt(i -> i).sum();
        int mergeTotal = 0;
        for (String output : outputs) {
            mergeTotal += HgkvDirImpl.open(output).numEntries();
        }
        Assert.assertEquals(total, mergeTotal);
    }

    private static RandomAccessInput sortBuffer(Sorter sorter,
                                                RandomAccessInput input)
                                                throws Exception {
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                                                  IntValue::new,
                                                  new IntValueSumCombiner());
        InnerSortFlusher flusher = new CombineKvInnerSortFlusher(output,
                                                                 combiner);
        sorter.sortBuffer(input, flusher, false);
        return EntriesUtil.inputFromOutput(output);
    }

    private static void mergeBuffers(Sorter sorter,
                                     List<RandomAccessInput> buffers,
                                     String output) throws Exception {
        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                                                  IntValue::new,
                                                  new IntValueSumCombiner());
        OuterSortFlusher flusher = new CombineKvOuterSortFlusher(combiner);
        sorter.mergeBuffers(buffers, flusher, output, false);
    }

    private static void mergeFiles(Sorter sorter, List<String> files,
                                   List<String> outputs) throws Exception {
        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                                                  IntValue::new,
                                                  new IntValueSumCombiner());
        OuterSortFlusher flusher = new CombineKvOuterSortFlusher(combiner);
        sorter.mergeInputs(files, flusher, outputs, false);
    }

    private static long sumOfEntryValue(Sorter sorter, List<String> files)
                                        throws Exception {
        long entrySize = 0L;
        for (String file : files) {
            HgkvDir dir = HgkvDirImpl.open(file);
            entrySize += dir.numEntries();
        }
        LOG.info("Finally kvEntry size: {}", entrySize);

        try (PeekableIterator<KvEntry> iterator = sorter.iterator(files,
                                                                  false)) {
            long result = 0;
            while (iterator.hasNext()) {
                KvEntry next = iterator.next();
                result += StoreTestUtil.dataFromPointer(next.value());
            }
            return result;
        }
    }

    private static void assertFileOrder(Sorter sorter, List<String> files)
                                        throws Exception {
        KvEntry last = null;
        try (PeekableIterator<KvEntry> iterator =
                                       sorter.iterator(files, false)) {
            while (iterator.hasNext()) {
                KvEntry next = iterator.next();
                if (last == null) {
                    last = iterator.next();
                    continue;
                }
                Assert.assertLte(0, last.key().compareTo(next.key()));
            }
        }
    }
}
