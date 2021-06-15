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

import java.util.NoSuchElementException;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.SubKvEntriesInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutputImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntryWriter;
import com.baidu.hugegraph.testutil.Assert;

public class EntriesUtilTest {

    @Test
    public void testSubKvEntriesInput() throws Exception {
        BytesOutput output = IOFactory.createBytesOutput(
                             Constants.SMALL_BUF_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(output);

        KvEntryWriter subKvWriter = entryOutput.writeEntry(new LongId(100));
        subKvWriter.writeSubKv(new LongId(20), new LongId(1));
        subKvWriter.writeSubKv(new LongId(10), new LongId(1));
        subKvWriter.writeSubKv(new LongId(50), new LongId(1));
        subKvWriter.writeSubKv(new LongId(40), new LongId(1));
        subKvWriter.writeSubKv(new LongId(10), new LongId(1));
        subKvWriter.writeFinish();

        BytesInput input = EntriesUtil.inputFromOutput(output);

        // Test inlinePointer kvEntry
        KvEntry entry = EntriesUtil.kvEntryFromInput(input, true, true);
        Assert.assertEquals(100,
                            StoreTestUtil.dataFromPointer(entry.key())
                                         .intValue());
        try (EntryIterator iter = new SubKvEntriesInput(entry, true)) {
            Assert.assertEquals(10,
                                StoreTestUtil.dataFromPointer(iter.next().key())
                                             .intValue());
            Assert.assertEquals(10,
                                StoreTestUtil.dataFromPointer(iter.next().key())
                                             .intValue());
            Assert.assertEquals(20,
                                StoreTestUtil.dataFromPointer(iter.next().key())
                                             .intValue());
            Assert.assertEquals(40,
                                StoreTestUtil.dataFromPointer(iter.next().key())
                                             .intValue());
            Assert.assertEquals(50,
                                StoreTestUtil.dataFromPointer(iter.next().key())
                                             .intValue());
        }

        input.seek(0);

        // Test cachedPointer kvEntry
        entry = EntriesUtil.kvEntryFromInput(input, false, true);
        Assert.assertEquals(100,
                            StoreTestUtil.dataFromPointer(entry.key())
                                         .intValue());
        try (EntryIterator iter = new SubKvEntriesInput(entry, false)) {
            Assert.assertEquals(10,
                                StoreTestUtil.dataFromPointer(iter.next().key())
                                             .intValue());
            Assert.assertEquals(10,
                                StoreTestUtil.dataFromPointer(iter.next().key())
                                             .intValue());
            Assert.assertEquals(20,
                                StoreTestUtil.dataFromPointer(iter.next().key())
                                             .intValue());
            Assert.assertEquals(40,
                                StoreTestUtil.dataFromPointer(iter.next().key())
                                             .intValue());
            Assert.assertEquals(50,
                                StoreTestUtil.dataFromPointer(iter.next().key())
                                             .intValue());
            Assert.assertThrows(NoSuchElementException.class,
                                iter::next);
        }
    }
}
