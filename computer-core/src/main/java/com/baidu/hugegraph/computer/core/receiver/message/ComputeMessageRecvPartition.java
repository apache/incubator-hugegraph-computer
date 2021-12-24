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

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.combiner.MessageValueCombiner;
import com.baidu.hugegraph.computer.core.combiner.PointerCombiner;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvPartition;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.KvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.computer.core.store.SuperstepFileGenerator;

public class ComputeMessageRecvPartition extends MessageRecvPartition {

    private static final String TYPE = MessageType.MSG.name().toLowerCase();

    private final OuterSortFlusher flusher;

    public ComputeMessageRecvPartition(ComputerContext context,
                                       SuperstepFileGenerator fileGenerator,
                                       SortManager sortManager) {
        super(context.config(), fileGenerator, sortManager, false);
        Config config = context.config();
        Combiner<Value> combiner = config.createObject(
                                   ComputerOptions.WORKER_COMBINER_CLASS,
                                   false);
        if (combiner == null) {
            this.flusher = new KvOuterSortFlusher();
        } else {
            PointerCombiner pointerCombiner = new MessageValueCombiner(context);
            this.flusher = new CombineKvOuterSortFlusher(pointerCombiner);
        }
    }

    @Override
    protected OuterSortFlusher outerSortFlusher() {
        return this.flusher;
    }

    @Override
    protected String type() {
        return TYPE;
    }
}
