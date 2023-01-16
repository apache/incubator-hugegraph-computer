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

package org.apache.hugegraph.computer.algorithm.community.cc;

import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.output.hg.HugeGraphOutput;
import org.apache.hugegraph.structure.constant.WriteType;

/**
 * Offer 2 ways to output: write-back + hdfs-file(TODO)
 */
public class ClusteringCoefficientOutput extends HugeGraphOutput<Float> {

    @Override
    public String name() {
        return "clustering_coefficient";
    }

    @Override
    public void prepareSchema() {
        this.client().schema().propertyKey(this.name())
            .asFloat()
            .writeType(WriteType.OLAP_RANGE)
            .ifNotExist()
            .create();
    }

    @Override
    protected org.apache.hugegraph.structure.graph.Vertex constructHugeVertex(Vertex vertex) {
        org.apache.hugegraph.structure.graph.Vertex hugeVertex =
                new org.apache.hugegraph.structure.graph.Vertex(null);
        hugeVertex.id(vertex.id().asObject());
        float triangle = ((ClusteringCoefficientValue) vertex.value()).count();
        int degree = ((ClusteringCoefficientValue) vertex.value()).idSet().value().size();
        hugeVertex.property(this.name(), 2 * triangle / degree / (degree - 1));
        return hugeVertex;
    }

    /* TODO: enhance it
    @Override
    protected Float value(Vertex vertex) {
        float triangle = ((ClusteringCoefficientValue) vertex.value()).count();
        int degree = ((ClusteringCoefficientValue) vertex.value()).idList().size();
        return 2 * triangle / degree / (degree - 1);
    }*/
}
