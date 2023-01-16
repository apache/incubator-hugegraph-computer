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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.io.BytesInput;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.Writable;
import org.apache.hugegraph.computer.core.sort.SorterTestUtil;
import org.apache.hugegraph.computer.core.store.buffer.KvEntriesInput;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.EntryOutput;
import org.apache.hugegraph.computer.core.store.entry.EntryOutputImpl;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.entry.KvEntryWriter;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class EntryOutputTest {

    @Test
    public void testWriteKvEntry() throws Exception {
        List<Integer> entries = ImmutableList.of(1, 5,
                                                 6, 6,
                                                 2, 1,
                                                 4, 8);
        List<Id> data = intListToLongIds(entries);

        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(output);

        for (int i = 0; i < data.size(); ) {
            Id id = data.get(i++);
            Writable value = data.get(i++);
            entryOutput.writeEntry(id, value);
        }

        // Assert result
        BytesInput input = EntriesUtil.inputFromOutput(output);
        EntryIterator iter = new KvEntriesInput(input);
        SorterTestUtil.assertKvEntry(iter.next(), BytesId.of(1), BytesId.of(5));
        SorterTestUtil.assertKvEntry(iter.next(), BytesId.of(6), BytesId.of(6));
        SorterTestUtil.assertKvEntry(iter.next(), BytesId.of(2), BytesId.of(1));
        SorterTestUtil.assertKvEntry(iter.next(), BytesId.of(4), BytesId.of(8));
        iter.close();
    }

    @Test
    public void testSubKvNotNeedSort() throws Exception {
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
        Id key1 = StoreTestUtil.idFromPointer(kvEntry1.key());
        Assert.assertEquals(BytesId.of(5), key1);
        EntryIterator kvEntry1SubKvs = EntriesUtil.subKvIterFromEntry(kvEntry1);
        KvEntry subKv1 = kvEntry1SubKvs.next();
        Assert.assertEquals(0, subKv1.numSubEntries());
        SorterTestUtil.assertKvEntry(subKv1, BytesId.of(6), BytesId.of(6));
        SorterTestUtil.assertKvEntry(kvEntry1SubKvs.next(),
                                     BytesId.of(2), BytesId.of(1));
        SorterTestUtil.assertKvEntry(kvEntry1SubKvs.next(),
                                     BytesId.of(4), BytesId.of(8));
        // Assert entry2
        KvEntry kvEntry2 = iter.next();
        Id key2 = StoreTestUtil.idFromPointer(kvEntry2.key());
        Assert.assertEquals(BytesId.of(1), key2);
        EntryIterator kvEntry2SubKvs = EntriesUtil.subKvIterFromEntry(kvEntry2);
        SorterTestUtil.assertKvEntry(kvEntry2SubKvs.next(),
                                     BytesId.of(2), BytesId.of(2));
        SorterTestUtil.assertKvEntry(kvEntry2SubKvs.next(),
                                     BytesId.of(6), BytesId.of(1));

        iter.close();
    }

    @Test
    public void testSubKvNeedSort() throws Exception {
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
        Id key1 = StoreTestUtil.idFromPointer(kvEntry1.key());
        Assert.assertEquals(BytesId.of(5), key1);

        EntryIterator kvEntry1SubKvs = EntriesUtil.subKvIterFromEntry(kvEntry1);
        KvEntry subKv1 = kvEntry1SubKvs.next();
        Assert.assertEquals(0, subKv1.numSubEntries());
        SorterTestUtil.assertKvEntry(subKv1, BytesId.of(2), BytesId.of(1));
        KvEntry subKv2 = kvEntry1SubKvs.next();
        Assert.assertEquals(0, subKv2.numSubEntries());
        SorterTestUtil.assertKvEntry(subKv2, BytesId.of(4), BytesId.of(8));
        KvEntry subKv3 = kvEntry1SubKvs.next();
        Assert.assertEquals(0, subKv3.numSubEntries());
        SorterTestUtil.assertKvEntry(subKv3, BytesId.of(6), BytesId.of(6));
        // Assert entry2
        KvEntry kvEntry2 = iter.next();
        Id key2 = StoreTestUtil.idFromPointer(kvEntry2.key());
        Assert.assertEquals(BytesId.of(1), key2);

        EntryIterator kvEntry2SubKvs = EntriesUtil.subKvIterFromEntry(kvEntry2);
        SorterTestUtil.assertKvEntry(kvEntry2SubKvs.next(),
                                     BytesId.of(2), BytesId.of(2));
        SorterTestUtil.assertKvEntry(kvEntry2SubKvs.next(),
                                     BytesId.of(6), BytesId.of(1));

        iter.close();
    }

    private static BytesInput inputFromEntries(List<Integer> entries,
                                               boolean needSort)
                                               throws IOException {
        /*
         * All integer data will convert to Id type, so upper layer also
         * needs to use the Id type to make a judgment
         */
        List<Id> data = intListToLongIds(entries);

        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
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

    private static List<Id> intListToLongIds(List<Integer> list) {
        return list.stream()
                   .map(BytesId::of)
                   .collect(Collectors.toList());
    }
}
