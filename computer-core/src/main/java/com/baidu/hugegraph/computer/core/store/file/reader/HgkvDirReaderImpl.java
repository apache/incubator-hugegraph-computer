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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.store.util.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.CloseableIterator;
import com.baidu.hugegraph.computer.core.store.entry.DefaultKvEntry;
import com.baidu.hugegraph.computer.core.store.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.file.HgkvDir;
import com.baidu.hugegraph.computer.core.store.file.HgkvFile;
import com.baidu.hugegraph.computer.core.store.file.HgkvDirImpl;

public class HgkvDirReaderImpl implements HgkvDirReader {

    private final HgkvDir hgkvDir;

    public HgkvDirReaderImpl(String path) throws IOException {
        this.hgkvDir = HgkvDirImpl.open(path);
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
        private CloseableIterator<Pointer> keyIter;
        private Pointer last;

        public KvEntryIter(HgkvDir hgkvDir) throws IOException {
            this.segments = hgkvDir.segments().iterator();
            this.numEntries = hgkvDir.entriesSize();
        }

        @Override
        public boolean hasNext() {
            return this.hasNextKey() || this.last != null;
        }

        @Override
        public KvEntry next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Pointer temp;
            List<Pointer> sameKeyValues = new ArrayList<>();
            try {
                if (this.last == null) {
                    this.last = this.nextKey();
                }
                sameKeyValues.add(EntriesUtil.valuePointerByKeyPointer(
                                              this.last));
                temp = this.last;

                if (!this.hasNextKey()) {
                    this.last = null;
                }
                // Get all values corresponding to the same key
                while (this.hasNextKey()) {
                    Pointer current = this.nextKey();
                    if (current.compareTo(this.last) == 0) {
                        sameKeyValues.add(EntriesUtil.valuePointerByKeyPointer(
                                          current));
                    } else {
                        this.last = current;
                        break;
                    }
                    if (!this.hasNextKey()) {
                        this.last = null;
                        break;
                    }
                }
            } catch (IOException e) {
                throw new ComputerException(e.getMessage(), e);
            }

            return new DefaultKvEntry(temp, sameKeyValues);
        }

        @Override
        public void close() throws IOException {
            this.keyIter.close();
        }

        private CloseableIterator<Pointer> nextKeyIter() throws IOException {
            CloseableIterator<Pointer> iterator;
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

        private Pointer nextKey() throws IOException {
            if (this.keyIter == null) {
                this.keyIter = this.nextKeyIter();
            }
            if (!this.keyIter.hasNext()) {
                this.keyIter.close();
                this.keyIter = this.nextKeyIter();
            }
            this.numEntries--;
            return this.keyIter.next();
        }

        private boolean hasNextKey() {
            return this.numEntries > 0;
        }
    }
}