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

import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.combiner.PointerCombiner;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvPartition;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineKvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.computer.core.store.SuperstepFileGenerator;

public class VertexMessageRecvPartition extends MessageRecvPartition {

    private static final String TYPE = MessageType.VERTEX.name().toLowerCase();

    private final OuterSortFlusher flusher;

    public VertexMessageRecvPartition(ComputerContext context,
                                      SuperstepFileGenerator fileGenerator,
                                      SortManager sortManager) {
        super(context.config(), fileGenerator, sortManager, false);
        Config config = context.config();
        /*
         * TODO: need improve because properties need to deserialize even if
         *  properties combiner is OverwritePropertiesCombiner
         */
        Combiner<Properties> propertiesCombiner = config.createObject(
                ComputerOptions.WORKER_VERTEX_PROPERTIES_COMBINER_CLASS);

        GraphFactory graphFactory = context.graphFactory();
        Properties v1 = graphFactory.createProperties();
        Properties v2 = graphFactory.createProperties();
        Properties result = graphFactory.createProperties();

        PointerCombiner<Properties> combiner = new PointerCombiner<>(
                                                   v1, v2, result,
                                                   propertiesCombiner);
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
