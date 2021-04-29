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
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.merge.HgkvDirMerger;
import com.baidu.hugegraph.computer.core.sort.merge.HgkvDirMergerImpl;
import com.baidu.hugegraph.computer.core.sort.sorter.InputsSorterImpl;
import com.baidu.hugegraph.computer.core.sort.sorter.ClosableInputsSorterImpl;
import com.baidu.hugegraph.computer.core.sort.sorter.InputSorter;
import com.baidu.hugegraph.computer.core.sort.sorter.JavaInputSorter;
import com.baidu.hugegraph.computer.core.sort.sorter.InputsSorter;
import com.baidu.hugegraph.computer.core.store.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.computer.core.store.file.builder.HgkvDirBuilderImpl;
import com.baidu.hugegraph.computer.core.store.file.reader.HgkvDirReaderImpl;
import com.baidu.hugegraph.computer.core.store.file.reader.HgkvDirReader;
import com.baidu.hugegraph.computer.core.store.file.reader.HgkvDir4SubKvReader;
import com.baidu.hugegraph.computer.core.store.iter.CloseableIterator;
import com.baidu.hugegraph.computer.core.store.iter.EntriesInput;
import com.baidu.hugegraph.computer.core.store.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.iter.EntriesSubKvInput;
import com.baidu.hugegraph.computer.core.store.select.DisperseEvenlySelector;
import com.baidu.hugegraph.computer.core.store.select.InputFilesSelector;
import com.baidu.hugegraph.computer.core.store.select.SelectedFiles;

public class SorterImpl implements Sorter {

    private final Config config;

    public SorterImpl(Config config) {
        this.config = config;
    }

    @Override
    public void sortBuffer(RandomAccessInput input,
                           InnerSortFlusher flusher) throws IOException {
        Iterator<KvEntry> entries = new EntriesInput(input);
        InputSorter sorter = new JavaInputSorter();
        flusher.flush(sorter.sort(entries));
    }

    @Override
    public void mergeBuffers(List<RandomAccessInput> inputs,
                             OuterSortFlusher flusher, String output,
                             boolean withSubKv) throws IOException {
        List<Iterator<KvEntry>> entries;
        if (withSubKv) {
            entries = inputs.stream()
                            .map(EntriesSubKvInput::new)
                            .collect(Collectors.toList());
        } else {
            entries = inputs.stream()
                            .map(EntriesInput::new)
                            .collect(Collectors.toList());
        }

        this.sortBuffers(entries, flusher, output);
    }

    @Override
    public void mergeInputs(List<String> inputs, OuterSortFlusher flusher,
                            List<String> outputs, boolean withSubKv) throws IOException {
        if (withSubKv) {
            this.mergeInputs(inputs, o -> new HgkvDir4SubKvReader(o).iterator(),
                             flusher, outputs);
        } else {
            this.mergeInputs(inputs, o -> new HgkvDirReaderImpl(o).iterator(),
                             flusher, outputs);
        }
    }

    @Override
    public CloseableIterator<KvEntry> iterator(List<String> inputs)
                                               throws IOException {
        ClosableInputsSorterImpl sorter = new ClosableInputsSorterImpl();
        List<CloseableIterator<KvEntry>> entries = new ArrayList<>();
        for (String input : inputs) {
            HgkvDirReader reader = new HgkvDirReaderImpl(input);
            entries.add(reader.iterator());
        }
        return sorter.sort(entries);
    }

    private void sortBuffers(List<Iterator<KvEntry>> entries,
                             OuterSortFlusher flusher,
                             String output) throws IOException {

        InputsSorter sorter = new InputsSorterImpl();
        try (HgkvDirBuilder builder = new HgkvDirBuilderImpl(output,
                                                             this.config)) {
            Iterator<KvEntry> result = sorter.sort(entries);
            flusher.flush(result, builder);
        }
    }

    private void mergeInputs(List<String> inputs,
                             Function<String,
                                     CloseableIterator<KvEntry>> inputToEntries,
                             OuterSortFlusher flusher, List<String> outputs)
                             throws IOException {
        InputFilesSelector selector = new DisperseEvenlySelector();
        // Each SelectedFiles include some input files per output.
        List<SelectedFiles> results = selector.selectedOfOutputs(inputs,
                                                                 outputs);

        HgkvDirMerger merger = new HgkvDirMergerImpl(this.config);
        for (SelectedFiles result : results) {
            merger.merge(result.inputs(), inputToEntries, result.output(),
                         flusher);
        }
    }
}
