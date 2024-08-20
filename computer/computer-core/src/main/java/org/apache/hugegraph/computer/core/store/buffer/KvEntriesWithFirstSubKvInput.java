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

package org.apache.hugegraph.computer.core.store.buffer;

import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.iterator.CIter;
import org.apache.hugegraph.iterator.MapperIterator;

public class KvEntriesWithFirstSubKvInput implements EntryIterator {

    private final CIter<KvEntry> entries;

    public KvEntriesWithFirstSubKvInput(RandomAccessInput input) {
        this.entries = new MapperIterator<>(
                       new KvEntriesInput(input),
                       EntriesUtil::kvEntryWithFirstSubKv);
    }

    @Override
    public boolean hasNext() {
        return this.entries.hasNext();
    }

    @Override
    public KvEntry next() {
        return this.entries.next();
    }

    @Override
    public void close() throws Exception {
        this.entries.close();
    }
}
