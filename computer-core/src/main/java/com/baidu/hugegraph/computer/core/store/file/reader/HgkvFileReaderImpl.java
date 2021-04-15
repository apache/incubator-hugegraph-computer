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

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.io.BufferedFileInput;
import com.baidu.hugegraph.computer.core.store.CloseableIterator;
import com.baidu.hugegraph.computer.core.store.base.DefaultPointer;
import com.baidu.hugegraph.computer.core.store.base.Pointer;
import com.baidu.hugegraph.computer.core.store.file.HgkvFile;
import com.baidu.hugegraph.computer.core.store.file.HgkvFileImpl;

public class HgkvFileReaderImpl implements HgkvFileReader {

    private final HgkvFile hgkvFile;

    public HgkvFileReaderImpl(String path) throws IOException {
        this.hgkvFile = HgkvFileImpl.open(path);
    }

    @Override
    public CloseableIterator<Pointer> iterator() throws IOException {
        return new Itr(this.hgkvFile);
    }

    private static class Itr implements CloseableIterator<Pointer> {

        private final BufferedFileInput input;
        private long numEntries;
        private long position;

        public Itr(HgkvFile hgkvFile) throws IOException {
            this.numEntries = hgkvFile.numEntries();
            this.input = new BufferedFileInput(new File(hgkvFile.path()));
            this.position = 0;
        }

        @Override
        public boolean hasNext() {
            return this.numEntries > 0;
        }

        @Override
        public Pointer next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Pointer pointer;
            try {
                this.input.seek(this.position);
                int keyLength = this.input.readInt();
                pointer = new DefaultPointer(this.input, this.input.position(),
                                             keyLength);
                this.input.skip(keyLength);
                this.input.skip(this.input.readInt());
                this.position = this.input.position();
            } catch (IOException e) {
                throw new ComputerException(e.getMessage(), e);
            }

            this.numEntries--;
            return pointer;
        }

        @Override
        public void close() throws IOException {
            this.input.close();
        }
    }
}
