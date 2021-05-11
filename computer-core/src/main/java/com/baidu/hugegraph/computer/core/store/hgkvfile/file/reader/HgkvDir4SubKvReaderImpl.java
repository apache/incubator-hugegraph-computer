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

package com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader;

import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.iterator.MapperIterator;

public class HgkvDir4SubKvReaderImpl implements HgkvDirReader {

    private final HgkvDirReader reader;

    public HgkvDir4SubKvReaderImpl(String path) {
        this.reader = new HgkvDirReaderImpl(path);
    }

    @Override
    public EntryIterator iterator() {
        return new KvEntryWithFirstSubKvIter(this.reader);
    }

    private static class KvEntryWithFirstSubKvIter implements EntryIterator {

        private final CIter<KvEntry> entries;

        public KvEntryWithFirstSubKvIter(HgkvDirReader reader) {
            this.entries = new MapperIterator<>(reader.iterator(), entry -> {
                return EntriesUtil.kvEntryWithFirstSubKv(entry);
            });
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
}
