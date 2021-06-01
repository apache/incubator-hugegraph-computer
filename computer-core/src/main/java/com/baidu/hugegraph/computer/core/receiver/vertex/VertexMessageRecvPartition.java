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

package com.baidu.hugegraph.computer.core.receiver.vertex;

import java.io.IOException;
import java.util.Collections;

import com.baidu.hugegraph.computer.core.combiner.OverwriteCombiner;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvPartition;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.FileGenerator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;

public class VertexMessageRecvPartition extends MessageRecvPartition {

    public static final String TYPE = MessageType.VERTEX.name();

    private OuterSortFlusher flusher;

    public VertexMessageRecvPartition(Config config,
                                      FileGenerator fileGenerator,
                                      Sorter sorter) {
        super(config, fileGenerator, sorter, Constants.INPUT_SUPERSTEP);
        // TODO: use customized properties combiner.
        this.flusher = new CombineKvOuterSortFlusher(new OverwriteCombiner<>());
    }

    @Override
    protected OuterSortFlusher outerSortFlusher() {
        return this.flusher;
    }

    @Override
    protected String type() {
        return TYPE;
    }

    @Override
    public PeekableIterator<KvEntry> iterator() throws IOException {
        this.flushAllBuffersAndWaitSorted();
        if (this.outputFiles().size() == 0) {
            return Collections.emptyIterator();
        }
        return this.sorter.iterator(this.outputFiles, false);
    }
}
