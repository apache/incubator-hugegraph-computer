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

package com.baidu.hugegraph.computer.algorithm.community.kcore;

import java.util.Iterator;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;

public class KCore implements Computation<Id> {

    public static final String ALGORITHM_NAME = "kcore";
    public static final String OPTION_K = "kcore.k";
    public static final int K_DEFAULT_VALUE = 3;

    private final KCoreValue initValue = new KCoreValue();

    private int k;

    @Override
    public String name() {
        return ALGORITHM_NAME;
    }

    @Override
    public String category() {
        return "community";
    }

    @Override
    public void init(Config config) {
        this.k = config.getInt(OPTION_K, K_DEFAULT_VALUE);
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        KCoreValue value = this.initValue;
        value.core(vertex.numEdges());
        vertex.value(value);

        if (value.core() < this.k) {
            value.core(0);
            context.sendMessageToAllEdges(vertex, vertex.id());
        }
        vertex.inactivate();
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<Id> messages) {
        KCoreValue value = vertex.value();
        if (!value.active()) {
            vertex.inactivate();
            return;
        }

        int deleted = 0;
        while (messages.hasNext()) {
            Id neighborId = messages.next();
            value.addDeletedNeighbor(neighborId);
            deleted++;
        }

        if (value.decreaseCore(deleted) < this.k) {
            value.core(0);
            context.sendMessageToAllEdgesIf(
                    vertex, vertex.id(),
                    (source, target) -> {
                        return !value.isNeighborDeleted(target);
                    });
        }
        vertex.inactivate();
    }
}
