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

package org.apache.hugegraph.computer.core.store.file.hgkvfile.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.KvEntryFileReader;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDir;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDirImpl;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvFile;

public class HgkvDirReaderImpl implements KvEntryFileReader {

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

        private final List<HgkvFile> segments;
        private final List<EntryIterator> segmentsIters;
        private int segmentIndex;
        private long numEntries;
        private EntryIterator kvIter;
        private final boolean useInlinePointer;
        private final boolean withSubKv;

        public HgkvDirEntryIter(HgkvDir hgkvDir, boolean useInlinePointer,
                                boolean withSubKv)
                                throws IOException {
            this.segments = hgkvDir.segments();
            this.segmentsIters = new ArrayList<>();
            this.segmentIndex = 0;
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
                if (this.kvIter == null || !this.kvIter.hasNext()) {
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
            for (EntryIterator iterator : this.segmentsIters) {
                iterator.close();
            }
            for (HgkvFile segment : this.segments) {
                segment.close();
            }
        }

        private EntryIterator nextKeyIter() throws Exception {
            HgkvFile segment = this.segments.get(this.segmentIndex++);
            KvEntryFileReader reader = new HgkvFileReaderImpl(
                                       segment.path(), this.useInlinePointer,
                                       this.withSubKv);
            EntryIterator iterator = reader.iterator();
            this.segmentsIters.add(iterator);
            return iterator;
        }

        private boolean hasNextKey() {
            return this.numEntries > 0;
        }
    }
}
