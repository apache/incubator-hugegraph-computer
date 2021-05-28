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

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDir;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvFile;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDirImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;

public class HgkvDirReaderImpl implements HgkvDirReader {

    private final HgkvDir hgkvDir;
    private final boolean useInlinePointer;
    private final boolean withSubKv;

    public HgkvDirReaderImpl(String path, boolean useInlinePointer,
                             boolean withSubKv) {
        try {
            this.hgkvDir = HgkvDirImpl.open(path);
            this.useInlinePointer = useInlinePointer;
            this.withSubKv = withSubKv;
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    public HgkvDirReaderImpl(String path, boolean withSubKv) {
        this(path, true, withSubKv);
    }

    public HgkvDirReaderImpl(String path) {
        this(path, true, false);
    }

    @Override
    public EntryIterator iterator() {
        try {
            return new HgkvDirEntryIter(this.hgkvDir, this.useInlinePointer,
                                        this.withSubKv);
        } catch (IOException e) {
            throw new ComputerException(e.getMessage(), e);
        }
    }

    private static class HgkvDirEntryIter implements EntryIterator {

        private final Iterator<HgkvFile> segments;
        private long numEntries;
        private EntryIterator kvIter;
        private final boolean useInlinePointer;
        private final boolean withSubKv;

        public HgkvDirEntryIter(HgkvDir hgkvDir, boolean useInlinePointer,
                                boolean withSubKv)
                                throws IOException {
            this.segments = hgkvDir.segments().iterator();
            this.numEntries = hgkvDir.numEntries();
            this.kvIter = null;
            this.useInlinePointer = useInlinePointer;
            this.withSubKv = withSubKv;
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
            } catch (Exception e) {
                throw new ComputerException(e.getMessage(), e);
            }
        }

        @Override
        public void close() throws Exception {
            this.kvIter.close();
        }

        private EntryIterator nextKeyIter() throws Exception {
            EntryIterator iterator;
            while (this.segments.hasNext()) {
                HgkvFile segment = this.segments.next();
                HgkvFileReader reader = new HgkvFileReaderImpl(
                                        segment.path(), this.useInlinePointer,
                                        this.withSubKv);
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
