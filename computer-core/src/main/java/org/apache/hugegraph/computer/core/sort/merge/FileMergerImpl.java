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

package org.apache.hugegraph.computer.core.sort.merge;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.sorter.InputsSorter;
import org.apache.hugegraph.computer.core.sort.sorter.InputsSorterImpl;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.KvEntryFileWriter;
import org.apache.hugegraph.computer.core.store.file.hgkvfile.HgkvDirImpl;
import org.apache.hugegraph.computer.core.util.FileUtil;
import org.apache.hugegraph.util.E;

public class FileMergerImpl implements FileMerger {

    private final int mergePathNum;
    private final String tempDir;

    public FileMergerImpl(Config config) {
        this.mergePathNum = config.get(ComputerOptions.HGKV_MERGE_FILES_NUM);
        this.tempDir = config.get(ComputerOptions.HGKV_TEMP_DIR) +
                       File.separator + UUID.randomUUID();
        boolean result = new File(this.tempDir).mkdirs();
        E.checkState(result, "Failed to create temp directory: '%s'",
                     this.tempDir);
    }

    @Override
    public void merge(List<String> inputs,
                      Function<String, EntryIterator> inputToEntries,
                      String output,
                      Function<String, KvEntryFileWriter> fileToWriter,
                      OuterSortFlusher flusher) throws Exception {
        List<String> subInputs = new ArrayList<>(this.mergePathNum);
        int round = 0;
        while (inputs.size() > this.mergePathNum) {
            List<String> newInputs = new ArrayList<>(inputs.size());
            for (int i = 0; i < inputs.size(); i++) {
                subInputs.add(inputs.get(i));
                if (subInputs.size() == this.mergePathNum ||
                    i == inputs.size() - 1) {
                    String subOutput = this.mergeInputsToRandomFile(
                            subInputs, inputToEntries,
                            fileToWriter, flusher);
                    // Don't remove original file
                    if (round != 0) {
                        FileUtil.deleteFilesQuietly(subInputs);
                    }
                    subInputs.clear();
                    newInputs.add(subOutput);
                }
            }

            inputs = newInputs;
            round++;
        }

        this.mergeInputs(inputs, inputToEntries, flusher, output, fileToWriter);
    }

    private String mergeInputsToRandomFile(
                   List<String> inputs,
                   Function<String, EntryIterator> inputToIter,
                   Function<String, KvEntryFileWriter> fileToWriter,
                   OuterSortFlusher flusher) throws Exception {
        String output = this.randomPath();
        this.mergeInputs(inputs, inputToIter, flusher, output, fileToWriter);
        return output;
    }

    private void mergeInputs(List<String> inputs,
                             Function<String, EntryIterator> inputToIter,
                             OuterSortFlusher flusher, String output,
                             Function<String, KvEntryFileWriter> fileToWriter)
                             throws Exception {
        /*
         * File value format is different, upper layer is required to
         * provide the file reading mode
         */
        List<EntryIterator> entries = inputs.stream()
                                            .map(inputToIter)
                                            .collect(Collectors.toList());

        InputsSorter sorter = new InputsSorterImpl();
        // Merge inputs and write to output
        try (EntryIterator sortedKv = sorter.sort(entries);
             KvEntryFileWriter builder = fileToWriter.apply(output)) {
             flusher.flush(sortedKv, builder);
        }
    }

    private String randomPath() {
        return this.tempDir + File.separator + HgkvDirImpl.FILE_NAME_PREFIX +
               UUID.randomUUID() + HgkvDirImpl.FILE_EXTEND_NAME;
    }
}
