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

package com.baidu.hugegraph.computer.core.sort.sorter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.sort.sorting.LoserTreeInputsSorting;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.util.E;

public class SubKvSorter implements Iterator<KvEntry> {

    private final PeekableIterator<KvEntry> kvEntries;
    private final int subKvSortPathNum;
    private final List<Iterator<KvEntry>> subKvMergeSource;
    private Iterator<KvEntry> subKvSorting;
    private KvEntry currentEntry;

    public SubKvSorter(PeekableIterator<KvEntry> kvEntries,
                       int subKvSortPathNum) {
        E.checkArgument(kvEntries.hasNext(),
                        "Parameter entries must not be empty");
        E.checkArgument(subKvSortPathNum > 0,
                        "Parameter subKvSortPathNum must > 0");
        this.kvEntries = kvEntries;
        this.subKvSortPathNum = subKvSortPathNum;
        this.subKvMergeSource = new ArrayList<>(this.subKvSortPathNum);
        this.init();
    }

    @Override
    public boolean hasNext() {
        return this.subKvSorting.hasNext();
    }

    @Override
    public KvEntry next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        return this.subKvSorting.next();
    }

    public KvEntry currentKv() {
        return this.currentEntry;
    }

    public void reset() {
        if (!this.kvEntries.hasNext()) {
            this.currentEntry = null;
            return;
        }
        this.subKvMergeSource.clear();

        assert this.subKvSortPathNum > 0;
        KvEntry entry;
        while (true) {
            entry = this.kvEntries.next();
            this.subKvMergeSource.add(new MergePath(this.kvEntries, entry));

            KvEntry next = this.kvEntries.peek();
            if (this.subKvMergeSource.size() == this.subKvSortPathNum ||
                next == null || entry.key().compareTo(next.key()) != 0) {
                break;
            }
        }
        this.subKvSorting = new LoserTreeInputsSorting<>(this.subKvMergeSource);
        this.currentEntry = entry;
    }

    private void init() {
        this.reset();
    }

    private static class MergePath implements Iterator<KvEntry> {

        private final PeekableIterator<KvEntry> entries;
        private Iterator<KvEntry> subKvIter;
        private KvEntry currentEntry;

        public MergePath(PeekableIterator<KvEntry> entries,
                         KvEntry currentEntry) {
            this.entries = entries;
            this.subKvIter = EntriesUtil.subKvIterFromEntry(currentEntry);
            this.currentEntry = currentEntry;
        }

        @Override
        public boolean hasNext() {
            return this.subKvIter.hasNext();
        }

        @Override
        public KvEntry next() {
            KvEntry nextSubKvEntry = this.subKvIter.next();
            if (!this.subKvIter.hasNext()) {
                KvEntry nextEntry = this.entries.peek();
                if (nextEntry != null &&
                    nextEntry.key().compareTo(this.currentEntry.key()) == 0) {
                    this.currentEntry = this.entries.next();
                    this.subKvIter = EntriesUtil.subKvIterFromEntry(
                                                 this.currentEntry);
                }
            }
            return nextSubKvEntry;
        }
    }
}
