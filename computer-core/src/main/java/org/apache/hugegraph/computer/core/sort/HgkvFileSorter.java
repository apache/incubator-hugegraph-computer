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
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.KvEntryFileWriter;
import org.apache.hugegraph.computer.core.store.buffer.KvEntriesInput;
import org.apache.hugegraph.computer.core.store.buffer.KvEntriesWithFirstSubKvInput;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.builder.HgkvDirBuilderImpl;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.reader.HgkvDir4SubKvReaderImpl;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.reader.HgkvDirReaderImpl;
import org.apache.hugegraph.computer.core.store.file.select.DisperseEvenlySelector;
import org.apache.hugegraph.computer.core.store.file.select.InputFilesSelector;
import org.apache.hugegraph.computer.core.store.file.select.SelectedFiles;

public class HgkvFileSorter implements Sorter {

    private final Config config;
    private final DefaultSorter sorter;

    public HgkvFileSorter(Config config) {
        this.config = config;
        this.sorter = new DefaultSorter(config);
    }

    @Override
    public void sortBuffer(RandomAccessInput input, InnerSortFlusher flusher,
                           boolean withSubKv) throws Exception {
        try (EntryIterator entries = new KvEntriesInput(input, withSubKv)) {
            this.sorter.sortBuffer(entries, flusher);
        }
    }

    @Override
    public void mergeBuffers(List<RandomAccessInput> inputs,
                             OuterSortFlusher flusher, String output,
                             boolean withSubKv) throws Exception {
        List<EntryIterator> entries;
        if (withSubKv) {
            entries = inputs.stream()
                            .map(KvEntriesWithFirstSubKvInput::new)
                            .collect(Collectors.toList());
        } else {
            entries = inputs.stream()
                            .map(KvEntriesInput::new)
                            .collect(Collectors.toList());
        }
        try (KvEntryFileWriter writer = new HgkvDirBuilderImpl(this.config,
                                                               output)) {
            this.sorter.mergeBuffers(entries, writer, flusher);
        }
    }

    @Override
    public void mergeInputs(List<String> inputs, OuterSortFlusher flusher,
                            List<String> outputs, boolean withSubKv)
                            throws Exception {
        Function<String, EntryIterator> fileToInput;
        Function<String, KvEntryFileWriter> fileToWriter;
        if (withSubKv) {
            fileToInput = o -> new HgkvDir4SubKvReaderImpl(o).iterator();
        } else {
            fileToInput = o -> new HgkvDirReaderImpl(o).iterator();
        }
        fileToWriter = path -> new HgkvDirBuilderImpl(this.config, path);

        InputFilesSelector selector = new DisperseEvenlySelector();
        List<SelectedFiles> selectResult = selector.selectedByHgkvFile(
                                                    inputs, outputs);
        this.sorter.mergeFile(selectResult, fileToInput, fileToWriter, flusher);
    }

    @Override
    public PeekableIterator<KvEntry> iterator(List<String> inputs,
                                              boolean withSubKv)
                                              throws IOException {
        Function<String, EntryIterator> fileToEntries = input -> {
            return new HgkvDirReaderImpl(input, false, withSubKv).iterator();
        };
        return this.sorter.iterator(inputs, fileToEntries);
    }
}
