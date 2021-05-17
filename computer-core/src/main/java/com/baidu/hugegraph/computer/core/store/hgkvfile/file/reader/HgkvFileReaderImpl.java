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

import com.baidu.hugegraph.computer.core.io.BufferedFileInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvFile;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvFileImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;

public class HgkvFileReaderImpl implements HgkvFileReader {

    private final HgkvFile hgkvFile;
    private final boolean useInput;

    public HgkvFileReaderImpl(String path, boolean useInput)
                              throws IOException {
        this.hgkvFile = HgkvFileImpl.open(path);
        this.useInput = useInput;
    }

    @Override
    public EntryIterator iterator() throws IOException {
        return new EntryIter(this.hgkvFile, this.useInput);
    }

    private static class EntryIter implements EntryIterator {

        private final BufferedFileInput input;
        private final BufferedFileInput userAccessInput;
        private long numEntries;
        private final boolean useInput;

        public EntryIter(HgkvFile hgkvFile, boolean useInput)
                         throws IOException {
            this.numEntries = hgkvFile.numEntries();
            File file = new File(hgkvFile.path());
            this.input = new BufferedFileInput(file);
            this.userAccessInput = this.input.duplicate();
            this.useInput = useInput;
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
            return EntriesUtil.entryFromInput(this.input,
                                              this.userAccessInput,
                                              this.useInput);
        }

        @Override
        public void close() throws IOException {
            this.input.close();
            this.userAccessInput.close();
        }
    }
}
