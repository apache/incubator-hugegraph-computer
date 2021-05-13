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

import com.baidu.hugegraph.computer.core.sort.flusher.PeekNextIter;
import com.baidu.hugegraph.computer.core.sort.sorting.LoserTreeInputsSorting;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.util.E;

public class SubKvSorter implements Iterator<KvEntry> {

    private final PeekNextIter<KvEntry> kvEntries;
    private final int subKvSortPathNum;
    private final List<Iterator<KvEntry>> subKvMergeSource;
    private Iterator<KvEntry> subKvSorting;

    public SubKvSorter(PeekNextIter<KvEntry> kvEntries, int subKvSortPathNum) {
        E.checkArgument(kvEntries.hasNext(),
                        "Parameter entries must not be empty");
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

    public void reset() {
        if (!this.kvEntries.hasNext()) {
            return;
        }
        this.subKvMergeSource.clear();

        KvEntry entry = this.kvEntries.next();
        this.subKvMergeSource.add(new MergePath(this.kvEntries, entry));
        while (this.subKvMergeSource.size() < this.subKvSortPathNum &&
               entry.key().compareTo(this.kvEntries.peekNext().key()) == 0) {
            entry = this.kvEntries.next();
            this.subKvMergeSource.add(new MergePath(this.kvEntries, entry));
        }
        this.subKvSorting = new LoserTreeInputsSorting<>(this.subKvMergeSource);
    }

    private void init() {
        this.reset();
    }

    private static class MergePath implements Iterator<KvEntry> {

        private final PeekNextIter<KvEntry> entries;
        private Iterator<KvEntry> subKvIter;
        private KvEntry current;

        public MergePath(PeekNextIter<KvEntry> entries, KvEntry current) {
            this.entries = entries;
            this.subKvIter = EntriesUtil.subKvIterFromEntry(current);
            this.current = current;
        }

        @Override
        public boolean hasNext() {
            return this.subKvIter.hasNext();
        }

        @Override
        public KvEntry next() {
            KvEntry next = this.subKvIter.next();
            if (!this.subKvIter.hasNext()) {
                KvEntry nextKvEntry = this.entries.peekNext();
                if (nextKvEntry != null &&
                    nextKvEntry.key().compareTo(this.current.key()) == 0) {
                    this.current = this.entries.next();
                    this.subKvIter = EntriesUtil.subKvIterFromEntry(
                                                 this.current);
                }
            }
            return next;
        }
    }
}
