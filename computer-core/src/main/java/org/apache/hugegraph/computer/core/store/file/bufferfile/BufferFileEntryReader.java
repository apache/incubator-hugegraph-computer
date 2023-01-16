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

package org.apache.hugegraph.computer.core.store.file.bufferfile;

import java.io.File;
import java.io.IOException;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.KvEntryFileReader;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;

public class BufferFileEntryReader implements KvEntryFileReader {

    private final File file;
    private final boolean withSubKv;

    public BufferFileEntryReader(String path, boolean withSubKv) {
        this.file = new File(path);
        this.withSubKv = withSubKv;
    }

    public BufferFileEntryReader(String path) {
        this(path, false);
    }

    @Override
    public EntryIterator iterator() {
        return new EntryIter();
    }

    private class EntryIter implements EntryIterator {

        public final RandomAccessInput input;
        private final RandomAccessInput userAccessInput;

        public EntryIter() {
            try {
                this.input = IOFactory.createFileInput(
                                       BufferFileEntryReader.this.file);
                this.userAccessInput = this.input.duplicate();
            } catch (IOException e) {
                throw new ComputerException(e.getMessage(), e);
            }
        }

        @Override
        public void close() throws Exception {
            this.input.close();
            this.userAccessInput.close();
        }

        @Override
        public boolean hasNext() {
            try {
                return this.input.available() > 0;
            } catch (IOException e) {
                throw new ComputerException(e.getMessage(), e);
            }
        }

        @Override
        public KvEntry next() {
            return EntriesUtil.kvEntryFromInput(
                               this.input, this.userAccessInput, true,
                               BufferFileEntryReader.this.withSubKv);
        }
    }
}
