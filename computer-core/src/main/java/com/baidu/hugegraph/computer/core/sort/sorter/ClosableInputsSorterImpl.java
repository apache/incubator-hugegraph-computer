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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.baidu.hugegraph.computer.core.sort.sorting.InputsSorting;
import com.baidu.hugegraph.computer.core.sort.sorting.SortingFactory;
import com.baidu.hugegraph.computer.core.store.iter.CloseableIterator;
import com.baidu.hugegraph.computer.core.store.entry.KvEntry;

public class ClosableInputsSorterImpl implements ClosableInputsSorter {

    @Override
    public CloseableIterator<KvEntry> sort(
                                      List<CloseableIterator<KvEntry>> entries)
                                      throws IOException {
        return new Sorting<>(entries);
    }

    private static class Sorting<T> implements CloseableIterator<T> {

        private final InputsSorting<T> inputsSorting;
        private final List<? extends CloseableIterator<T>> sources;
        private boolean closed;

        public Sorting(List<? extends CloseableIterator<T>> sources) {
            this.sources = sources;
            this.inputsSorting = SortingFactory.createSorting(sources);
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            return this.inputsSorting.hasNext();
        }

        @Override
        public T next() {
            return this.inputsSorting.next();
        }

        @Override
        public void close() throws IOException {
            if (this.closed) {
                return;
            }
            for (Closeable source : this.sources) {
                source.close();
            }
            this.closed = true;
        }
    }
}
