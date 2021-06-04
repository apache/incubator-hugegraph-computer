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

package com.baidu.hugegraph.computer.core.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIteratorAdaptor;
import com.baidu.hugegraph.computer.core.sort.merge.HgkvDirMerger;
import com.baidu.hugegraph.computer.core.sort.merge.HgkvDirMergerImpl;
import com.baidu.hugegraph.computer.core.sort.sorter.InputsSorterImpl;
import com.baidu.hugegraph.computer.core.sort.sorter.InputSorter;
import com.baidu.hugegraph.computer.core.sort.sorter.JavaInputSorter;
import com.baidu.hugegraph.computer.core.sort.sorter.InputsSorter;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.InputToEntries;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReaderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReader;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDir4SubKvReaderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.KvEntriesInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.KvEntriesWithFirstSubKvInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.select.DisperseEvenlySelector;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.select.InputFilesSelector;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.select.SelectedFiles;

public class SorterImpl implements Sorter {

    private final Config config;

    public SorterImpl(Config config) {
        this.config = config;
    }

    @Override
    public void sortBuffer(RandomAccessInput input, InnerSortFlusher flusher,
                           boolean withSubKv) throws Exception {
        try (EntryIterator entries = new KvEntriesInput(input, withSubKv)) {
            InputSorter sorter = new JavaInputSorter();
            flusher.flush(sorter.sort(entries));
        }
    }

    @Override
    public void mergeBuffers(List<RandomAccessInput> inputs,
                             OuterSortFlusher flusher, String output,
                             boolean withSubKv) throws Exception {
        List<EntryIterator> entries = null;
        try {
            if (withSubKv) {
                entries = inputs.stream()
                                .map(KvEntriesWithFirstSubKvInput::new)
                                .collect(Collectors.toList());
            } else {
                entries = inputs.stream()
                                .map(KvEntriesInput::new)
                                .collect(Collectors.toList());
            }
        } finally {
            if (entries != null) {
                for (EntryIterator iterator : entries) {
                    iterator.close();
                }
            }
        }

        this.sortBuffers(entries, flusher, output);
    }

    @Override
    public void mergeInputs(List<String> inputs, OuterSortFlusher flusher,
                            List<String> outputs, boolean withSubKv)
                            throws Exception {
        InputToEntries inputToEntries;
        if (withSubKv) {
            inputToEntries = o -> new HgkvDir4SubKvReaderImpl(o).iterator();
        } else {
            inputToEntries = o -> new HgkvDirReaderImpl(o).iterator();
        }
        this.mergeInputs(inputs, inputToEntries, flusher, outputs);
    }

    @Override
    public PeekableIterator<KvEntry> iterator(List<String> inputs,
                                              boolean withSubKv)
                                              throws IOException {
        InputsSorterImpl sorter = new InputsSorterImpl();
        List<EntryIterator> entries = new ArrayList<>();
        for (String input : inputs) {
            HgkvDirReader reader = new HgkvDirReaderImpl(input, false,
                                                         withSubKv);
            entries.add(reader.iterator());
        }
        return PeekableIteratorAdaptor.of(sorter.sort(entries));
    }

    private void sortBuffers(List<EntryIterator> entries,
                             OuterSortFlusher flusher, String output)
                             throws IOException {
        InputsSorter sorter = new InputsSorterImpl();
        try (HgkvDirBuilder builder = new HgkvDirBuilderImpl(this.config,
                                                             output)) {
            EntryIterator result = sorter.sort(entries);
            flusher.flush(result, builder);
        }
    }

    private void mergeInputs(List<String> inputs, InputToEntries inputToEntries,
                             OuterSortFlusher flusher, List<String> outputs)
                             throws Exception {
        InputFilesSelector selector = new DisperseEvenlySelector();
        // Each SelectedFiles include some input files per output.
        List<SelectedFiles> results = selector.selectedOfOutputs(inputs,
                                                                 outputs);

        HgkvDirMerger merger = new HgkvDirMergerImpl(this.config);
        for (SelectedFiles result : results) {
            merger.merge(result.inputs(), inputToEntries,
                         result.output(), flusher);
        }
    }
}
