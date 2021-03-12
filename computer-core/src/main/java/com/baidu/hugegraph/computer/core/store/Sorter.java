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

package com.baidu.hugegraph.computer.core.store;

import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public interface Sorter {

    /**
     * Sort the buffer by increasing order of key.
     * The input buffer format: | key1 length | key1 | value1 length | value1 |
     * key2 length | key2 | value2 length | value2 | and so on. If a same key
     * exists several time, combine the value before output.
     * @param input The input buffer.
     * @param valueCombiner The combiner for the same key.
     * @return sorted buffer.
     */
    long sortBuffer(HgkvInput input,
                    SortCombiner valueCombiner,
                    RandomAccessOutput output);

    /**
     * Merge the buffers by increasing order of key.
     * The input buffer in list is in increasing order, it is in format:
     * | key1 length | key1 | value1 length | value1 | key2 length | key2 |
     * value2 length | value2 | and so on.
     * @param inputBuffers The input buffer list.
     * @param valueCombiner The combiner for the same key.
     * @param output sorted output. The output may be a buffer or a file.
     * @return the number of distinct keys after merge.
     */
    long mergeBuffers(List<HgkvInput> inputBuffers,
                      SortCombiner valueCombiner,
                      RandomAccessOutput output);

    /**
     * Merge the n inputs into m outputs.
     * The content in inputs is | key1 length | key1 | value1 length | value1 |,
     * by increasing order of the key.
     * For example number of the inputs is 100, and m is 10, this method
     * merge 100 inputs into 10 outputs.
     * @return The outputs merged.
     */
    void mergeInputs(List<HgkvInput> inputs, List<HgkvOutput> outputs);

    /**
     * Get the iterator of <key, value> pair by increasing order of key.
     */
    Iterator<KvEntry> iterator(List<HgkvInput> inputs);
}
