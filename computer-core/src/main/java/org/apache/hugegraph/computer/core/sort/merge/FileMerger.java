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

import java.util.List;
import java.util.function.Function;

import org.apache.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.KvEntryFileWriter;

public interface FileMerger {

    /**
     * Merge inputs file to output file
     * @param inputs file that need to be merged
     * @param inputToEntries key value pair read mode
     * @param output write merge result to this file
     * @param flusher combiner entries of same key
     */
    void merge(List<String> inputs,
               Function<String, EntryIterator> inputToEntries,
               String output, Function<String, KvEntryFileWriter> fileToWriter,
               OuterSortFlusher flusher) throws Exception;
}
