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

package com.baidu.hugegraph.computer.core.receiver.message;

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvPartition;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.store.FileGenerator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;

public class ComputeMessageRecvPartition extends MessageRecvPartition {

    public static final String TYPE = MessageType.MSG.name();

    public ComputeMessageRecvPartition(Config config,
                                       FileGenerator fileGenerator,
                                       Sorter sorter,
                                       int superstep) {
        super(config, fileGenerator, sorter, superstep);
    }

    @Override
    protected OuterSortFlusher outerSortFlusher() {
        return null;
    }

    @Override
    protected String type() {
        return TYPE;
    }

    @Override
    public Iterator<KvEntry> iterator() {
        return null;
    }
}
