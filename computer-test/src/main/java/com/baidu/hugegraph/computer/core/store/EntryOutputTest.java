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

package com.baidu.hugegraph.computer.core.store;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.computer.core.sort.SorterTestUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.KvEntriesInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutputImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntryWriter;
import com.google.common.collect.ImmutableList;

public class EntryOutputTest {

    @Test
    public void testWriteKvEntry() throws IOException {
        List<Integer> entries = ImmutableList.of(1, 5,
                                                 6, 6,
                                                 2, 1,
                                                 4, 8);
        List<LongId> data = intListToLongIds(entries);

        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.DEFAULT_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(output);

        for (int i = 0; i < data.size(); ) {
            LongId id = data.get(i++);
            Writable value = data.get(i++);
            entryOutput.writeEntry(id, value);
        }

        // Assert result
        BytesInput input = EntriesUtil.inputFromOutput(output);
        EntryIterator iter = new KvEntriesInput(input);
        SorterTestUtil.assertKvEntry(iter.next(), 1, 5);
        SorterTestUtil.assertKvEntry(iter.next(), 6, 6);
        SorterTestUtil.assertKvEntry(iter.next(), 2, 1);
        SorterTestUtil.assertKvEntry(iter.next(), 4, 8);
    }

    @Test
    public void testSubKvNotNeedSort() throws IOException {
        List<Integer> entries = ImmutableList.of(5,
                                                 6, 6,
                                                 2, 1,
                                                 4, 8,
                                                 1,
                                                 2, 2,
                                                 6, 1);
        BytesInput input = inputFromEntries(entries, false);
        EntryIterator iter = new KvEntriesInput(input, true);

        // Assert entry1
        KvEntry kvEntry1 = iter.next();
        Assert.assertEquals(3, kvEntry1.numSubEntries());
        int key1 = StoreTestUtil.dataFromPointer(kvEntry1.key());
        Assert.assertEquals(5, key1);
        EntryIterator kvEntry1SubKvs = EntriesUtil.subKvIterFromEntry(kvEntry1);
        KvEntry subKv1 = kvEntry1SubKvs.next();
        Assert.assertEquals(0, subKv1.numSubEntries());
        SorterTestUtil.assertKvEntry(subKv1, 6, 6);
        SorterTestUtil.assertKvEntry(kvEntry1SubKvs.next(), 2, 1);
        SorterTestUtil.assertKvEntry(kvEntry1SubKvs.next(), 4, 8);
        // Assert entry2
        KvEntry kvEntry2 = iter.next();
        int key2 = StoreTestUtil.dataFromPointer(kvEntry2.key());
        Assert.assertEquals(1, key2);
        EntryIterator kvEntry2SubKvs = EntriesUtil.subKvIterFromEntry(kvEntry2);
        SorterTestUtil.assertKvEntry(kvEntry2SubKvs.next(), 2, 2);
        SorterTestUtil.assertKvEntry(kvEntry2SubKvs.next(), 6, 1);
    }

    @Test
    public void testSubKvNeedSort() throws IOException {
        List<Integer> entries = ImmutableList.of(5,
                                                 6, 6,
                                                 2, 1,
                                                 4, 8,
                                                 1,
                                                 2, 2,
                                                 6, 1);
        BytesInput input = inputFromEntries(entries, true);
        EntryIterator iter = new KvEntriesInput(input, true);

        // Assert entry1
        KvEntry kvEntry1 = iter.next();
        int key1 = StoreTestUtil.dataFromPointer(kvEntry1.key());
        Assert.assertEquals(5, key1);

        EntryIterator kvEntry1SubKvs = EntriesUtil.subKvIterFromEntry(kvEntry1);
        KvEntry subKv1 = kvEntry1SubKvs.next();
        Assert.assertEquals(0, subKv1.numSubEntries());
        SorterTestUtil.assertKvEntry(subKv1, 2, 1);
        KvEntry subKv2 = kvEntry1SubKvs.next();
        Assert.assertEquals(0, subKv2.numSubEntries());
        SorterTestUtil.assertKvEntry(subKv2, 4, 8);
        KvEntry subKv3 = kvEntry1SubKvs.next();
        Assert.assertEquals(0, subKv3.numSubEntries());
        SorterTestUtil.assertKvEntry(subKv3, 6, 6);
        // Assert entry2
        KvEntry kvEntry2 = iter.next();
        int key2 = StoreTestUtil.dataFromPointer(kvEntry2.key());
        Assert.assertEquals(1, key2);

        EntryIterator kvEntry2SubKvs = EntriesUtil.subKvIterFromEntry(kvEntry2);
        SorterTestUtil.assertKvEntry(kvEntry2SubKvs.next(), 2, 2);
        SorterTestUtil.assertKvEntry(kvEntry2SubKvs.next(), 6, 1);
    }

    private static BytesInput inputFromEntries(List<Integer> entries,
                                               boolean needSort)
                                               throws IOException {
        List<LongId> data = intListToLongIds(entries);

        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.DEFAULT_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(output, needSort);
        int index = 0;
        KvEntryWriter entry1 = entryOutput.writeEntry(data.get(index++));
        entry1.writeSubKv(data.get(index++), data.get(index++));
        entry1.writeSubKv(data.get(index++), data.get(index++));
        entry1.writeSubKv(data.get(index++), data.get(index++));
        entry1.writeFinish();
        KvEntryWriter entry2 = entryOutput.writeEntry(data.get(index++));
        entry2.writeSubKv(data.get(index++), data.get(index++));
        entry2.writeSubKv(data.get(index++), data.get(index));
        entry2.writeFinish();

        return EntriesUtil.inputFromOutput(output);
    }

    private static List<LongId> intListToLongIds(List<Integer> list) {
        return list.stream()
                   .map(LongId::new)
                   .collect(Collectors.toList());
    }
}
