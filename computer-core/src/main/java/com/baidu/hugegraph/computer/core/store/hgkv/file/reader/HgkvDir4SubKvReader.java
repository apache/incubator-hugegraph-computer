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

package com.baidu.hugegraph.computer.core.store.hgkv.file.reader;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.store.value.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.value.iter.InputIterator;
import com.baidu.hugegraph.computer.core.store.EntriesUtil;

public class HgkvDir4SubKvReader implements HgkvDirReader {

    private final HgkvDirReader reader;

    public HgkvDir4SubKvReader(String path) {
        this.reader = new HgkvDirReaderImpl(path);
    }

    @Override
    public InputIterator iterator() {
        try {
            return new KvEntryIter(this.reader);
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    private static class KvEntryIter implements InputIterator {

        private final InputIterator entries;

        public KvEntryIter(HgkvDirReader reader) throws IOException {
            this.entries = reader.iterator();
        }

        @Override
        public boolean hasNext() {
            return this.entries.hasNext();
        }

        @Override
        public KvEntry next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                return EntriesUtil.kvEntryWithFirstSubKv(this.entries.next());
            } catch (IOException e) {
                throw new ComputerException(e.getMessage(), e);
            }
        }

        @Override
        public void close() throws IOException {
            this.entries.close();
        }
    }
}
