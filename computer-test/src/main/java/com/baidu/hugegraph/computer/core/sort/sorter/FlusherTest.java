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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.sort.SorterTestUtil;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.KvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.KvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.store.StoreTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntriesInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;

public class FlusherTest {

    private static final Config CONFIG = ComputerContext.instance().config();

    @Test
    public void testKvInnerSortFlusher() throws Exception {
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
    public void testKvOuterSortFlusher() throws Exception {
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
        Iterator<KvEntry> iter = sorter.iterator(outputs, false);
        SorterTestUtil.assertKvEntry(iter.next(), 1, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 3, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 3, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 4, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 6, 1);
    }

    @Test
    public void testExceptionCaseForFlusher() {
        RandomAccessOutput output = new UnsafeBytesOutput();
        InnerSortFlusher flusher = new KvInnerSortFlusher(output);
        List<KvEntry> entries = new ArrayList<>();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            flusher.flush(entries.iterator());
        }, e -> {
            String errorMsg = "Parameter entries can't be empty";
            Assert.assertContains(errorMsg, e.getMessage());
        });
    }
}
