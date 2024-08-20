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

package org.apache.hugegraph.computer.core.sort.sorter;

import java.io.IOException;
import java.util.List;

import org.apache.hugegraph.computer.core.sort.sorting.InputsSorting;
import org.apache.hugegraph.computer.core.sort.sorting.SortingFactory;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;

public class InputsSorterImpl implements InputsSorter {

    @Override
    public EntryIterator sort(List<EntryIterator> entries) throws IOException {
        return new SortingEntries(entries);
    }

    private static class SortingEntries implements EntryIterator {

        private final InputsSorting<KvEntry> inputsSorting;
        private final List<EntryIterator> sources;
        private boolean closed;

        public SortingEntries(List<EntryIterator> sources) {
            this.sources = sources;
            this.inputsSorting = SortingFactory.createSorting(sources);
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            return this.inputsSorting.hasNext();
        }

        @Override
        public KvEntry next() {
            return this.inputsSorting.next();
        }

        @Override
        public void close() throws Exception {
            if (this.closed) {
                return;
            }
            for (EntryIterator source : this.sources) {
                source.close();
            }
            this.closed = true;
        }
    }
}
