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

package org.apache.hugegraph.computer.core.sort;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIteratorAdaptor;
import org.apache.hugegraph.computer.core.sort.merge.FileMerger;
import org.apache.hugegraph.computer.core.sort.merge.FileMergerImpl;
import org.apache.hugegraph.computer.core.sort.sorter.InputSorter;
import org.apache.hugegraph.computer.core.sort.sorter.InputsSorter;
import org.apache.hugegraph.computer.core.sort.sorter.InputsSorterImpl;
import org.apache.hugegraph.computer.core.sort.sorter.JavaInputSorter;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.KvEntryFileWriter;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.file.select.SelectedFiles;

public class DefaultSorter {

    private final Config config;

    public DefaultSorter(Config config) {
        this.config = config;
    }

    public void sortBuffer(EntryIterator entries, InnerSortFlusher flusher)
                           throws Exception {
        InputSorter sorter = new JavaInputSorter();
        flusher.flush(sorter.sort(entries));
    }

    public void mergeBuffers(List<EntryIterator> entries,
                             KvEntryFileWriter writer, OuterSortFlusher flusher)
                             throws IOException {
        InputsSorter sorter = new InputsSorterImpl();
        EntryIterator result = sorter.sort(entries);
        flusher.flush(result, writer);
    }

    public void mergeFile(List<SelectedFiles> selectedFiles,
                          Function<String, EntryIterator> fileToEntries,
                          Function<String, KvEntryFileWriter> fileToWriter,
                          OuterSortFlusher flusher) throws Exception {
        FileMerger merger = new FileMergerImpl(this.config);
        for (SelectedFiles select : selectedFiles) {
            merger.merge(select.inputs(), fileToEntries,
                         select.output(), fileToWriter, flusher);
        }
    }

    public PeekableIterator<KvEntry> iterator(
           List<String> inputs, Function<String, EntryIterator> fileToEntries)
           throws IOException {
        List<EntryIterator> entries = inputs.stream()
                                            .map(fileToEntries)
                                            .collect(Collectors.toList());
        InputsSorterImpl sorter = new InputsSorterImpl();
        return PeekableIteratorAdaptor.of(sorter.sort(entries));
    }
}
