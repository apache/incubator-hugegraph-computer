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
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.combiner.MockIntSumCombiner;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.store.StoreTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDirImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReader;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReaderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Bytes;
import com.google.common.collect.Lists;

public class LargeDataSizeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(
                                                       LargeDataSizeTest.class);
    private static String FILE_DIR;
    private static StopWatch watcher;

    @BeforeClass
    public static void init() {
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MERGE_PATH_NUM, "200",
                ComputerOptions.HGKV_MAX_FILE_SIZE, String.valueOf(Bytes.GB)
        );
        FILE_DIR = System.getProperty("user.home") + File.separator + "hgkv";
        watcher = new StopWatch();
    }

    @Test
    public void test() throws Exception {
        FileUtils.deleteQuietly(new File(FILE_DIR));

        final long bufferSize = Bytes.MB;
        final int mergeBufferNum = 300;
        final int dataSize = 1000000;
        long value = 0;

        Random random = new Random();
        UnsafeBytesOutput output = new UnsafeBytesOutput();
        List<RandomAccessInput> buffers = new ArrayList<>(mergeBufferNum);
        List<String> mergeBufferFiles = new ArrayList<>();
        int fileNum = 10;
        Sorter sorter = new SorterImpl(ComputerContext.instance().config());

        watcher.start();
        for (int i = 0; i < dataSize; i++) {
            output.writeInt(Integer.BYTES);
            output.writeInt(random.nextInt(dataSize));
            output.writeInt(Integer.BYTES);
            int entryValue = random.nextInt(5);
            value = value + entryValue;
            output.writeInt(entryValue);

            // Write data to buffer and sort buffer
            if (output.position() >= bufferSize || (i + 1) == dataSize) {
                UnsafeBytesInput input = EntriesUtil.inputFromOutput(output);
                buffers.add(sortBuffer(sorter, input));
                output = new UnsafeBytesOutput();
            }

            // Merge buffers to HgkvDir
            if (buffers.size() >= mergeBufferNum || (i + 1) == dataSize) {
                String outputFile = availableDirPath(String.valueOf(fileNum++));
                mergeBufferFiles.add(outputFile);
                mergeBuffers(sorter, buffers, outputFile);
                buffers.clear();
            }
        }

        // Merge file
        String resultFile1 = availableDirPath("0");
        mergeFiles(sorter, mergeBufferFiles, Lists.newArrayList(resultFile1));

        watcher.stop();
        LOGGER.info(String.format("LargeDataSizeTest sort time: %s",
                                  watcher.getTime()));

        long result = getFileValue(resultFile1);
        Assert.assertEquals(value, result);
    }

    private static RandomAccessInput sortBuffer(Sorter sorter,
                                                RandomAccessInput input)
                                                throws IOException {
        UnsafeBytesOutput output = new UnsafeBytesOutput();
        Combiner<Pointer> combiner = new MockIntSumCombiner();
        InnerSortFlusher flusher = new CombineKvInnerSortFlusher(output,
                                                                 combiner);
        sorter.sortBuffer(input, flusher);
        return EntriesUtil.inputFromOutput(output);
    }

    private static void mergeBuffers(Sorter sorter,
                                     List<RandomAccessInput> buffers,
                                     String output) throws IOException {
        Combiner<Pointer> combiner = new MockIntSumCombiner();
        OuterSortFlusher flusher = new CombineKvOuterSortFlusher(combiner);
        sorter.mergeBuffers(buffers, flusher, output, false);
    }

    private static void mergeFiles(Sorter sorter, List<String> files,
                                   List<String> outputs) throws Exception {
        Combiner<Pointer> combiner = new MockIntSumCombiner();
        OuterSortFlusher flusher = new CombineKvOuterSortFlusher(combiner);
        sorter.mergeInputs(files, flusher, outputs, false);
    }

    private static long getFileValue(String file) throws IOException {
        HgkvDirReader reader = new HgkvDirReaderImpl(file, false);
        EntryIterator iterator = reader.iterator();
        long result = 0;
        while (iterator.hasNext()) {
            KvEntry next = iterator.next();
            result += StoreTestUtil.dataFromPointer(next.value());
        }
        return result;
    }

    private static String availableDirPath(String id) {
        return FILE_DIR + File.separator + HgkvDirImpl.FILE_NAME_PREFIX + id +
               HgkvDirImpl.FILE_EXTEND_NAME;
    }
}
