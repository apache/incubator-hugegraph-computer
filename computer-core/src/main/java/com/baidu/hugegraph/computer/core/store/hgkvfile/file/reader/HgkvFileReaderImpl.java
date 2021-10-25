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

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvFile;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvFileImpl;

public class HgkvFileReaderImpl implements HgkvFileReader {

    private final HgkvFile hgkvFile;
    private final boolean useInlinePointer;
    private final boolean withSubKv;

    public HgkvFileReaderImpl(String path, boolean useInlinePointer,
                              boolean withSubKv)
                              throws IOException {
        this.hgkvFile = HgkvFileImpl.open(path);
        this.useInlinePointer = useInlinePointer;
        this.withSubKv = withSubKv;
    }

    public HgkvFileReaderImpl(String path, boolean withSubKv)
                              throws IOException {
        this(path, true, withSubKv);
    }

    @Override
    public EntryIterator iterator() throws IOException {
        return new EntryIter(this.hgkvFile, this.useInlinePointer,
                             this.withSubKv);
    }

    private static class EntryIter implements EntryIterator {

        private final HgkvFile file;
        private final RandomAccessInput input;
        private final RandomAccessInput userAccessInput;
        private long numEntries;
        private final boolean useInlinePointer;
        private final boolean withSubKv;

        public EntryIter(HgkvFile hgkvFile, boolean useInlinePointer,
                         boolean withSubKv)
                         throws IOException {
            this.file = hgkvFile;
            this.numEntries = this.file.numEntries();
            File file = new File(this.file.path());
            this.input = IOFactory.createFileInput(file);
            this.userAccessInput = this.input.duplicate();
            this.useInlinePointer = useInlinePointer;
            this.withSubKv = withSubKv;
        }

        @Override
        public boolean hasNext() {
            return this.numEntries > 0;
        }

        @Override
        public KvEntry next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            this.numEntries--;
            return EntriesUtil.kvEntryFromInput(this.input,
                                                this.userAccessInput,
                                                this.useInlinePointer,
                                                this.withSubKv);
        }

        @Override
        public void close() throws IOException {
            this.file.close();
            this.input.close();
            this.userAccessInput.close();
        }
    }
}
