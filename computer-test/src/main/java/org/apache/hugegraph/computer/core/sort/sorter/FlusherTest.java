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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.computer.core.combiner.OverwriteCombiner;
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
import org.apache.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.KvInnerSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.KvOuterSortFlusher;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.StoreTestUtil;
import org.apache.hugegraph.computer.core.store.buffer.KvEntriesInput;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class FlusherTest {

    private static Config CONFIG;

    @BeforeClass
    public static void init() {
        CONFIG = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
    }

    @Test
    public void testKvInnerSortFlusher() throws Exception {
        List<Integer> map = ImmutableList.of(2, 1,
                                             3, 1,
                                             2, 1,
                                             4, 1);
        BytesInput input = SorterTestUtil.inputFromKvMap(map);

        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        Sorter sorter = SorterTestUtil.createSorter(CONFIG);
        sorter.sortBuffer(input, new KvInnerSortFlusher(output), false);

        BytesInput result = EntriesUtil.inputFromOutput(output);
        EntryIterator iter = new KvEntriesInput(result);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 3, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 4, 1);
        iter.close();
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
        BytesInput input1 = SorterTestUtil.inputFromKvMap(map1);
        BytesInput input2 = SorterTestUtil.inputFromKvMap(map2);
        List<RandomAccessInput> inputs = ImmutableList.of(input1, input2);

        String resultFile = StoreTestUtil.availablePathById("1");
        Sorter sorter = SorterTestUtil.createSorter(CONFIG);
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
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        InnerSortFlusher flusher = new KvInnerSortFlusher(output);
        List<KvEntry> entries = new ArrayList<>();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            flusher.flush(entries.iterator());
        }, e -> {
            String errorMsg = "Parameter entries can't be empty";
            Assert.assertContains(errorMsg, e.getMessage());
        });
    }

    @Test
    public void testOverwriteCombiner() throws Exception {
        List<Integer> data = ImmutableList.of(1, 2,
                                              3, 5,
                                              1, 3,
                                              1, 1,
                                              3, 4);
        BytesInput input = SorterTestUtil.inputFromKvMap(data);

        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        PointerCombiner combiner = SorterTestUtil.createPointerCombiner(
                                                  IntValue::new,
                                                  new OverwriteCombiner<>());
        InnerSortFlusher flusher = new CombineKvInnerSortFlusher(output,
                                                                 combiner);
        Sorter sorter = SorterTestUtil.createSorter(CONFIG);
        sorter.sortBuffer(input, flusher, false);

        BytesInput result = EntriesUtil.inputFromOutput(output);
        // Assert result
        KvEntriesInput iter = new KvEntriesInput(result);
        SorterTestUtil.assertKvEntry(iter.next(), 1, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 3, 4);
        iter.close();
    }
}
