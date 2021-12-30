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

package com.baidu.hugegraph.computer.algorithm.centrality.closeness;

import java.util.Map;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.hg.HugeOutput;
import com.baidu.hugegraph.structure.constant.WriteType;
import com.baidu.hugegraph.util.Log;

public class ClosenessCentralityOutput extends HugeOutput {

    private static final Logger LOG =
            Log.logger(ClosenessCentralityOutput.class);

    @Override
    public void prepareSchema() {
        this.client().schema().propertyKey(this.name())
                     .asDouble()
                     .writeType(WriteType.OLAP_RANGE)
                     .ifNotExist()
                     .create();
    }

    @Override
    public com.baidu.hugegraph.structure.graph.Vertex constructHugeVertex(
                                                      Vertex vertex) {
        LOG.info("The closeness centrality aaa\n");

        com.baidu.hugegraph.structure.graph.Vertex hugeVertex =
                new com.baidu.hugegraph.structure.graph.Vertex(null);
        hugeVertex.id(vertex.id().asObject());

        LOG.info("The closeness centrality bbb\n");

        // TODO：How to get the total vertices count here?
        // long n = context.totalVertexCount() - 1;
        ClosenessValue localValue = vertex.value();
        // Cumulative distance
        double centrality = 0;
        for (Map.Entry<Id, DoubleValue> entry : localValue.entrySet()) {
            centrality += 1.0D / entry.getValue().value();
        }
        LOG.info("The closeness centrality ccc\n");
        hugeVertex.property(this.name(), centrality);
        LOG.info("The closeness centrality of vertex {} is {}",
                 vertex, centrality);
        return hugeVertex;
    }
}
