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
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayInput;
import com.baidu.hugegraph.computer.core.io.UnsafeByteArrayOutput;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.MockOutSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.store.StoreTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDirImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReader;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReaderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Bytes;
import com.google.common.collect.Lists;

public class LargeDataSizeTest {

    private static String FILE_DIR;

    @BeforeClass
    public static void init() {
        Watcher.start("init");
        // Don't forget to register options
        OptionSpace.register("computer",
                             "com.baidu.hugegraph.computer.core.config." +
                             "ComputerOptions");
        UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.HGKV_MERGE_PATH_NUM, "200",
                ComputerOptions.HGKV_MAX_FILE_SIZE, String.valueOf(Bytes.GB)
        );
        FILE_DIR = System.getProperty("user.home") + File.separator + "hgkv";
        Watcher.stop();
    }

    @After
    public void teardown() {
        Watcher.start("teardown");
        FileUtils.deleteQuietly(new File(FILE_DIR));
        Watcher.stop();
    }

    @Test
    public void test() throws Exception {
        final long bufferSize = Bytes.MB;
        final int mergeBufferNum = 300;
        final int dataSize = 1000000;
        long value = 0;

        Random random = new Random();
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        List<RandomAccessInput> buffers = new ArrayList<>(mergeBufferNum);
        List<String> mergeBufferFiles = new ArrayList<>();
        int fileNum = 10;
        Sorter sorter = new SorterImpl(ComputerContext.instance().config());

        Watcher.start("sort");
        for (int i = 0; i < dataSize; i++) {
            output.writeInt(Integer.BYTES);
            output.writeInt(random.nextInt(dataSize));
            output.writeInt(Integer.BYTES);
            int entryValue = random.nextInt(10);
            value = value + entryValue;
            output.writeInt(entryValue);

            // Write data to buffer and sort buffer
            if (output.position() >= bufferSize || (i + 1) == dataSize) {
                UnsafeByteArrayInput input = inputFromOutput(output);
                buffers.add(sortBuffer(sorter, input));
                output = new UnsafeByteArrayOutput();
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

        Watcher.stop();

        long result = getFileValue(resultFile1);
        System.out.println("expected:" + value);
        System.out.println("actual:" + result);
        Assert.assertEquals(value, result);
    }

    private static RandomAccessInput sortBuffer(Sorter sorter,
                                                RandomAccessInput input)
                                                throws IOException {
        UnsafeByteArrayOutput output = new UnsafeByteArrayOutput();
        InnerSortFlusher combiner = new MockInnerSortFlusher(output);
        sorter.sortBuffer(input, combiner);
        return inputFromOutput(output);
    }

    private static int getBufferValue(RandomAccessInput input)
                                      throws IOException {
        input.seek(0);

        int value = 0;
        for (KvEntry kvEntry : EntriesUtil.readInput(input)) {
            value += StoreTestUtil.dataFromPointer(kvEntry.value());
        }

        input.seek(0);

        return value;
    }

    private static long getFileValue(String file) throws IOException {
        Watcher.start("getFileValue");
        HgkvDirReader reader = new HgkvDirReaderImpl(file);
        EntryIterator iterator = reader.iterator();
        long result = 0;
        while (iterator.hasNext()) {
            KvEntry next = iterator.next();
            result += StoreTestUtil.dataFromPointer(next.value());
        }
        Watcher.stop();
        return result;
    }

    static class MockInnerSortFlusher implements InnerSortFlusher {

        private final RandomAccessOutput output;

        public MockInnerSortFlusher(RandomAccessOutput output) {
            this.output = output;
        }

        @Override
        public RandomAccessOutput output() {
            return this.output;
        }

        @Override
        public Combiner<KvEntry> combiner() {
            return null;
        }

        @Override
        public void flush(Iterator<KvEntry> entries) throws IOException {
            KvEntry last = entries.next();
            List<KvEntry> sameKeyEntries = new ArrayList<>();
            sameKeyEntries.add(last);

            while (true) {
                KvEntry current = null;
                if (entries.hasNext()) {
                    current = entries.next();
                    if (last.compareTo(current) == 0) {
                        sameKeyEntries.add(current);
                        continue;
                    }
                }

                Pointer key = sameKeyEntries.get(0).key();
                this.output.writeInt(Integer.BYTES);
                this.output.write(key.input(), key.offset(), key.length());
                int valueSum = 0;
                for (KvEntry entry : sameKeyEntries) {
                    Pointer value = entry.value();
                    valueSum += StoreTestUtil.dataFromPointer(value);
                }
                this.output.writeInt(Integer.BYTES);
                this.output.writeInt(valueSum);

                if (current == null) {
                    break;
                }

                sameKeyEntries.clear();
                sameKeyEntries.add(current);
                last = current;
            }
        }
    }

    private static void mergeBuffers(Sorter sorter,
                                     List<RandomAccessInput> buffers,
                                     String output)
                                     throws IOException {
        OuterSortFlusher combiner = new MockOutSortFlusher();
        sorter.mergeBuffers(buffers, combiner, output, false);
    }

    private static void mergeFiles(Sorter sorter, List<String> files,
                                   List<String> outputs) throws Exception {
        OuterSortFlusher combiner = new MockOutSortFlusher();
        sorter.mergeInputs(files, combiner, Lists.newArrayList(outputs),
                           false);
    }

    private static UnsafeByteArrayInput inputFromOutput(UnsafeByteArrayOutput output) {
        return new UnsafeByteArrayInput(output.buffer(), output.position());
    }

    private static String availableDirPath(String id) {
        return FILE_DIR + File.separator + HgkvDirImpl.FILE_NAME_PREFIX + id +
               HgkvDirImpl.FILE_EXTEND_NAME;
    }

    private static class Watcher {

        private static String doWhat;
        private static long time;

        public static void start(String doWhat) {
            Watcher.doWhat = doWhat;
            time = System.currentTimeMillis();
        }

        public static void stop() {
            long current = System.currentTimeMillis();
            System.out.printf("%s time:" + (current - time) + "\n", doWhat);
        }
    }
}
