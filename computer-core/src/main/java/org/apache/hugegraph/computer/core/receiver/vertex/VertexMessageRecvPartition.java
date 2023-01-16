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

package org.apache.hugegraph.computer.core.receiver.vertex;

import org.apache.hugegraph.computer.core.combiner.PointerCombiner;
import org.apache.hugegraph.computer.core.combiner.VertexValueCombiner;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.receiver.MessageRecvPartition;
import org.apache.hugegraph.computer.core.sort.flusher.CombineKvOuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.SuperstepFileGenerator;

public class VertexMessageRecvPartition extends MessageRecvPartition {

    private static final String TYPE = MessageType.VERTEX.name().toLowerCase();

    private final OuterSortFlusher flusher;

    public VertexMessageRecvPartition(ComputerContext context,
                                      SuperstepFileGenerator fileGenerator,
                                      SortManager sortManager) {
        super(context.config(), fileGenerator, sortManager, false);

        PointerCombiner combiner = new VertexValueCombiner(context);

        this.flusher = new CombineKvOuterSortFlusher(combiner);
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
