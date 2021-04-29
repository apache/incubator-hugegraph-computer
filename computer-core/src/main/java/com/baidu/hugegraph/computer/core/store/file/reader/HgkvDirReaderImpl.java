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

package com.baidu.hugegraph.computer.core.store.file.reader;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.store.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.iter.CloseableIterator;
import com.baidu.hugegraph.computer.core.store.file.HgkvDir;
import com.baidu.hugegraph.computer.core.store.file.HgkvFile;
import com.baidu.hugegraph.computer.core.store.file.HgkvDirImpl;

public class HgkvDirReaderImpl implements HgkvDirReader {

    private final HgkvDir hgkvDir;

    public HgkvDirReaderImpl(String path) {
        try {
            this.hgkvDir = HgkvDirImpl.open(path);
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    @Override
    public CloseableIterator<KvEntry> iterator() {
        try {
            return new KvEntryIter(this.hgkvDir);
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    private static class KvEntryIter implements CloseableIterator<KvEntry> {

        private final Iterator<HgkvFile> segments;
        private long numEntries;
        private CloseableIterator<KvEntry> kvIter;

        public KvEntryIter(HgkvDir hgkvDir) throws IOException {
            this.segments = hgkvDir.segments().iterator();
            this.numEntries = hgkvDir.numEntries();
        }

        @Override
        public boolean hasNext() {
            return this.hasNextKey();
        }

        @Override
        public KvEntry next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                if (this.kvIter == null) {
                    this.kvIter = this.nextKeyIter();
                }
                if (!this.kvIter.hasNext()) {
                    this.kvIter.close();
                    this.kvIter = this.nextKeyIter();
                }
                this.numEntries--;
                return this.kvIter.next();
            } catch (IOException e) {
                throw new ComputerException(e.getMessage(), e);
            }
        }

        @Override
        public void close() throws IOException {
            this.kvIter.close();
        }

        private CloseableIterator<KvEntry> nextKeyIter() throws IOException {
            CloseableIterator<KvEntry> iterator;
            while (this.segments.hasNext()) {
                HgkvFile segment = this.segments.next();
                HgkvFileReader reader = new HgkvFileReaderImpl(segment.path());
                iterator = reader.iterator();
                if (iterator.hasNext()) {
                    return iterator;
                } else {
                    iterator.close();
                }
            }
            throw new NoSuchElementException();
        }

        private boolean hasNextKey() {
            return this.numEntries > 0;
        }
    }
}
