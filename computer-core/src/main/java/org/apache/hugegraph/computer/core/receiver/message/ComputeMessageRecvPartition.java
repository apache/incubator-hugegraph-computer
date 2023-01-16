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

package org.apache.hugegraph.computer.core.receiver.message;

import org.apache.hugegraph.computer.core.combiner.Combiner;
import org.apache.hugegraph.computer.core.combiner.MessageValueCombiner;
import org.apache.hugegraph.computer.core.combiner.PointerCombiner;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.receiver.MessageRecvPartition;
import org.apache.hugegraph.computer.core.sort.flusher.CombineKvOuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.KvOuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.SuperstepFileGenerator;

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
