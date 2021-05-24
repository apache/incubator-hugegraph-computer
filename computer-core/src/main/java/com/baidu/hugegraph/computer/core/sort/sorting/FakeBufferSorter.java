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

package com.baidu.hugegraph.computer.core.sort.sorting;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.store.HgkvFile;
import com.baidu.hugegraph.computer.core.store.KvEntry;
import com.baidu.hugegraph.computer.core.store.SortCombiner;
import com.baidu.hugegraph.computer.core.store.Sorter;

public class FakeBufferSorter implements Sorter {

    @Override
    public void sortBuffer(RandomAccessInput input, SortCombiner valueCombiner,
                           RandomAccessOutput output) throws IOException {
        // Just write it intact
        output.write(input, 0, input.available());
    }

    @Override
    public void mergeBuffers(List<RandomAccessInput> inputBuffers,
                             SortCombiner valueCombiner, HgkvFile file) {

    }

    @Override
    public void mergeInputs(List<HgkvFile> inputs, SortCombiner valueCombiner,
                            List<HgkvFile> outputs) {

    }

    @Override
    public Iterator<KvEntry> iterator(List<HgkvFile> inputs) {
        return null;
    }
}
