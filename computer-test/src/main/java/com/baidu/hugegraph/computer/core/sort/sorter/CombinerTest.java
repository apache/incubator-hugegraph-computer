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

package com.baidu.hugegraph.computer.core.sort.sorter;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.sort.SorterTestUtil;
import com.baidu.hugegraph.computer.core.sort.combiner.OverrideCombiner;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvInnerSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.InnerSortFlusher;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntriesInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntriesUtil;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.google.common.collect.ImmutableList;

public class CombinerTest {

    private static final Config CONFIG = ComputerContext.instance().config();

    @Test
    public void testOverrideCombiner() throws Exception {
        List<Integer> data = ImmutableList.of(1, 2, 3, 5, 1, 3, 1, 1, 3, 4);
        UnsafeBytesInput input = SorterTestUtil.inputFromKvMap(data);

        UnsafeBytesOutput output = new UnsafeBytesOutput();
        Combiner<Pointer> combiner = new OverrideCombiner();
        InnerSortFlusher flusher = new CombineKvInnerSortFlusher(output,
                                                                 combiner);
        Sorter sorter = new SorterImpl(CONFIG);
        sorter.sortBuffer(input, flusher);

        UnsafeBytesInput result = EntriesUtil.inputFromOutput(output);
        // Assert result
        Iterator<KvEntry> kvIter = new EntriesInput(result);
        SorterTestUtil.assertKvEntry(kvIter.next(), 1, 1);
        SorterTestUtil.assertKvEntry(kvIter.next(), 3, 4);
    }
}
