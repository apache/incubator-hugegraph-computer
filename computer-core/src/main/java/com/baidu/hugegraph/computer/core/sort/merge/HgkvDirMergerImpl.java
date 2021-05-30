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

package com.baidu.hugegraph.computer.core.sort.merge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.sorter.InputsSorterImpl;
import com.baidu.hugegraph.computer.core.sort.sorter.InputsSorter;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.InputToEntries;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.HgkvDirImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilder;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.builder.HgkvDirBuilderImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.util.E;

public class HgkvDirMergerImpl implements HgkvDirMerger {

    private final Config config;
    private final int mergePathNum;
    private final String tempDir;

    public HgkvDirMergerImpl(Config config) {
        this.config = config;
        this.mergePathNum = config.get(ComputerOptions.HGKV_MERGE_FILES_NUM);
        this.tempDir = config.get(ComputerOptions.HGKV_TEMP_DIR) +
                       File.separator + UUID.randomUUID();
        boolean result = new File(this.tempDir).mkdirs();
        E.checkState(result, "Failed to create temp directory: '%s'",
                     this.tempDir);
    }

    @Override
    public void merge(List<String> inputs, InputToEntries inputToEntries,
                      String output, OuterSortFlusher flusher)
                      throws Exception {
        try {
            /*
             * Merge files until the number of temporary files
             * is less than MERGE_PATH_NUM
             */
            int tempFileId = 0;
            while (inputs.size() > this.mergePathNum) {
                // Split files to be merged
                List<List<String>> splitResult = this.splitSubInputs(inputs);
                // Merge split result
                List<File> tempFiles = this.mergeSubInputs(splitResult,
                                                           tempFileId,
                                                           inputToEntries,
                                                           flusher);
                // Prepare to start a new round of merge
                inputs = tempFiles.stream()
                                  .map(File::getPath)
                                  .collect(Collectors.toList());
                tempFileId += inputs.size();
            }

            // Merge the last set of temporary files into output
            this.mergeInputsToOutput(inputs, inputToEntries, output, flusher);
        } finally {
            // Delete temporary files
            FileUtils.deleteQuietly(new File(this.tempDir));
        }
    }

    private List<List<String>> splitSubInputs(List<String> inputs) {
        int splitResultSize = inputs.size() % this.mergePathNum == 0 ?
                              inputs.size() / this.mergePathNum :
                              inputs.size() / this.mergePathNum + 1;
        List<List<String>> splitResult = new ArrayList<>(splitResultSize);
        List<String> subInputs = new ArrayList<>(this.mergePathNum);

        for (int i = 0; i < inputs.size(); i++) {
            subInputs.add(inputs.get(i));
            if (subInputs.size() == this.mergePathNum ||
                i == inputs.size() - 1) {
                splitResult.add(new ArrayList<>(subInputs));
                subInputs.clear();
            }
        }

        return splitResult;
    }

    private List<File> mergeSubInputs(List<List<String>> splitResult,
                                      int tempFileId,
                                      InputToEntries inputToEntries,
                                      OuterSortFlusher flusher)
                                      throws Exception {
        List<File> tempFiles = new ArrayList<>(splitResult.size());
        // Merge subInputs
        for (List<String> subInputs : splitResult) {
            String fileName = this.filePathFromId(++tempFileId);
            File tempFile = this.mergeInputsToOutput(subInputs, inputToEntries,
                                                     fileName, flusher);
            tempFiles.add(tempFile);
        }
        return tempFiles;
    }

    private File mergeInputsToOutput(List<String> inputs,
                                     InputToEntries inputToEntries,
                                     String output,
                                     OuterSortFlusher flusher)
                                     throws Exception {
        /*
         * File value format is different, upper layer is required to
         * provide the file reading mode
         */
        List<EntryIterator> entries = new ArrayList<>();
        for (String input : inputs) {
            entries.add(inputToEntries.inputToEntries(input));
        }

        InputsSorter sorter = new InputsSorterImpl();
        File file = new File(output);
        // Merge inputs and write to output
        try (EntryIterator sortedKv = sorter.sort(entries);
             HgkvDirBuilder builder = new HgkvDirBuilderImpl(this.config,
                                                             file.getPath())) {
             flusher.flush(sortedKv, builder);
        } catch (IOException e) {
            FileUtils.deleteQuietly(file);
            throw e;
        }
        return file;
    }

    private String filePathFromId(int fileId) {
        return this.tempDir + File.separator + HgkvDirImpl.FILE_NAME_PREFIX +
               fileId + HgkvDirImpl.FILE_EXTEND_NAME;
    }

    @Override
    protected void finalize() throws Throwable {
        // Delete temporary folder when object is recycled
        FileUtils.deleteQuietly(new File(this.tempDir));
    }
}
