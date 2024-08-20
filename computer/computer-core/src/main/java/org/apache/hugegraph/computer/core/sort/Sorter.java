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

import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;

public interface Sorter {

    /**
     * Sort the buffer by increasing order of key. Every key exists only once
     * in output buffer.
     * The input buffer format:
     * | key1 length | key1 | value1 length | value1 |
     * | key2 length | key2 | value2 length | value2 |
     * | key1 length | key1 | value3 length | value3 |
     * and so on.
     * If some key exists several time, combine the values.
     * @param input The input buffer.
     * @param flusher The flusher for the same key.
     * @param withSubKv True if need sort subKv.
     */
    void sortBuffer(RandomAccessInput input, InnerSortFlusher flusher,
                    boolean withSubKv) throws Exception;

    /**
     * Merge the buffers by increasing order of key.
     * The input buffers in list are in increasing order of the key.
     * There are two formats for the input buffer:
     * 1.
     * | key1 length | key1 | value1 length | value1 |
     * | key2 length | key2 | value2 length | value2 |
     * and so on.
     * Keys are in increasing order in each buffer.
     * 2.
     * | key1 length | key1 | value1 length | sub-entry count |
     * | sub-key1 length | sub-key1 | sub-value1 length | sub-value1 |
     * | sub-key2 length | sub-key2 | sub-value2 length | sub-value2 |
     * and so on.
     * Keys are in increasing order in each buffer.
     * Sub-keys are in increasing order in a key value pair.
     *
     * The results of multiple buffer sorting are outputted to @param output
     * @param inputBuffers The input buffer list.
     * @param flusher The flusher for the same key.
     * @param output Sort result output location.
     * @param withSubKv True if need sort subKv.
     */
     void mergeBuffers(List<RandomAccessInput> inputBuffers,
                       OuterSortFlusher flusher, String output,
                       boolean withSubKv) throws Exception;

    /**
     * Merge the n inputs into m outputs.
     * 'n' is size of inputs, 'm' is size of outputs.
     * The input files in list are in increasing order of the key.
     * There are two formats for the input buffer:
     * 1.
     * | key1 length | key1 | value1 length | value1 |
     * | key2 length | key2 | value2 length | value2 |
     * and so on.
     * Keys are in increasing order in each buffer.
     * 2.
     * | key1 length | key1 | value1 length | sub-entry count |
     * | sub-key1 length | sub-key1 | sub-value1 length | sub-value1 |
     * | sub-key2 length | sub-key2 | sub-value2 length | sub-value2 |
     * and so on.
     * Sub-keys are in increasing order in a key value pair.
     *
     * The format of outputs is same as inputs.
     * For example number of the inputs is 100, and number of the outputs is
     * 10, this method merge 100 inputs into 10 outputs.
     * The outputs need to be as evenly distributed as possible. It might
     * need to sort the inputs by desc order. Then select the inputs one by
     * one assign to the output with least inputs. It makes the difference
     * between the outputs below the least inputs.
     * @param inputs The input file list.
     * @param flusher The flusher for the same key.
     * @param outputs Sort result output locations.
     * @param withSubKv True if need sort subKv.
     */
    void mergeInputs(List<String> inputs, OuterSortFlusher flusher,
                     List<String> outputs, boolean withSubKv) throws Exception;

    /**
     * Get the iterator of <key, value> pair by increasing order of key.
     */
    PeekableIterator<KvEntry> iterator(List<String> inputs, boolean withSubKv)
                                       throws IOException;
}
