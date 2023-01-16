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
import org.apache.hugegraph.computer.core.store.file.bufferfile.BufferFileEntryBuilder;
import org.apache.hugegraph.computer.core.store.file.bufferfile.BufferFileEntryReader;
import org.apache.hugegraph.computer.core.store.file.bufferfile.BufferFileSubEntryReader;
import org.apache.hugegraph.computer.core.store.file.select.DisperseEvenlySelector;
import org.apache.hugegraph.computer.core.store.file.select.InputFilesSelector;
import org.apache.hugegraph.computer.core.store.file.select.SelectedFiles;

public class BufferFileSorter implements Sorter {

    private final DefaultSorter sorter;

    public BufferFileSorter(Config config) {
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
        try (KvEntryFileWriter writer = new BufferFileEntryBuilder(output)) {
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
            fileToInput = o -> new BufferFileSubEntryReader(o).iterator();
        } else {
            fileToInput = o -> new BufferFileEntryReader(o).iterator();
        }
        fileToWriter = BufferFileEntryBuilder::new;

        InputFilesSelector selector = new DisperseEvenlySelector();
        List<SelectedFiles> selectResult = selector.selectedByBufferFile(
                                                    inputs, outputs);
        this.sorter.mergeFile(selectResult, fileToInput, fileToWriter, flusher);
    }

    @Override
    public PeekableIterator<KvEntry> iterator(List<String> inputs,
                                              boolean withSubKv)
                                              throws IOException {
        Function<String, EntryIterator> fileToEntries = input -> {
            return new BufferFileEntryReader(input, withSubKv).iterator();
        };
        return this.sorter.iterator(inputs, fileToEntries);
    }
}
