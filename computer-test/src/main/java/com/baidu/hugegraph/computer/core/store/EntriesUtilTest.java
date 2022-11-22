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
import java.util.NoSuchElementException;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.store.buffer.SubKvEntriesInput;
import com.baidu.hugegraph.computer.core.store.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.entry.EntryOutput;
import com.baidu.hugegraph.computer.core.store.entry.EntryOutputImpl;
import com.baidu.hugegraph.computer.core.store.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.entry.KvEntryWriter;
import org.apache.hugegraph.testutil.Assert;

public class EntriesUtilTest {

    @Test
    public void testSubKvEntriesInput() throws Exception {
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(output);

        KvEntryWriter subKvWriter = entryOutput.writeEntry(BytesId.of(100));
        subKvWriter.writeSubKv(BytesId.of(20), BytesId.of(1));
        subKvWriter.writeSubKv(BytesId.of(10), BytesId.of(1));
        subKvWriter.writeSubKv(BytesId.of(50), BytesId.of(1));
        subKvWriter.writeSubKv(BytesId.of(40), BytesId.of(1));
        subKvWriter.writeSubKv(BytesId.of(10), BytesId.of(1));
        subKvWriter.writeFinish();

        BytesInput input = EntriesUtil.inputFromOutput(output);

        // Test inlinePointer kvEntry
        KvEntry entry = EntriesUtil.kvEntryFromInput(input, true, true);
        Assert.assertEquals(BytesId.of(100),
                            StoreTestUtil.idFromPointer(entry.key()));
        try (EntryIterator iter = new SubKvEntriesInput(entry, true)) {
            Assert.assertEquals(BytesId.of(10),
                                StoreTestUtil.idFromPointer(iter.next().key()));
            Assert.assertEquals(BytesId.of(10),
                                StoreTestUtil.idFromPointer(iter.next().key()));
            Assert.assertEquals(BytesId.of(20),
                                StoreTestUtil.idFromPointer(iter.next().key()));
            Assert.assertEquals(BytesId.of(40),
                                StoreTestUtil.idFromPointer(iter.next().key()));
            Assert.assertEquals(BytesId.of(50),
                                StoreTestUtil.idFromPointer(iter.next().key()));
        }

        input.seek(0);

        // Test cachedPointer kvEntry
        entry = EntriesUtil.kvEntryFromInput(input, false, true);
        Assert.assertEquals(BytesId.of(100),
                            StoreTestUtil.idFromPointer(entry.key()));
        try (EntryIterator iter = new SubKvEntriesInput(entry, false)) {
            Assert.assertEquals(BytesId.of(10),
                                StoreTestUtil.idFromPointer(iter.next().key()));
            Assert.assertEquals(BytesId.of(10),
                                StoreTestUtil.idFromPointer(iter.next().key()));
            Assert.assertEquals(BytesId.of(20),
                                StoreTestUtil.idFromPointer(iter.next().key()));
            Assert.assertEquals(BytesId.of(40),
                                StoreTestUtil.idFromPointer(iter.next().key()));
            Assert.assertEquals(BytesId.of(50),
                                StoreTestUtil.idFromPointer(iter.next().key()));
            Assert.assertThrows(NoSuchElementException.class, iter::next);
        }
    }

    @Test
    public void testKvEntryWithFirstSubKv() throws IOException {
        BytesOutput output = IOFactory.createBytesOutput(
                                       Constants.SMALL_BUF_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(output);
        KvEntryWriter subKvWriter = entryOutput.writeEntry(BytesId.of(100));
        subKvWriter.writeSubKv(BytesId.of(1), BytesId.of(1));
        subKvWriter.writeSubKv(BytesId.of(1), BytesId.of(1));
        subKvWriter.writeSubKv(BytesId.of(1), BytesId.of(1));
        subKvWriter.writeSubKv(BytesId.of(1), BytesId.of(1));
        subKvWriter.writeFinish();

        BytesInput input = EntriesUtil.inputFromOutput(output);

        // Read entry from buffer
        KvEntry entry = EntriesUtil.kvEntryFromInput(input, true, true);
        entry = EntriesUtil.kvEntryWithFirstSubKv(entry);
        // Assert subKvEntry size
        Assert.assertEquals(4, entry.numSubEntries());
    }
}
