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

package com.baidu.hugegraph.computer.core.io;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public class CsvStructGraphOutput extends StructGraphOutput {

    public CsvStructGraphOutput(DataOutput out,
                                ComputerContext computerContext) {
        super(out, computerContext);
    }

    @Override
    public void writeVertex(Vertex vertex) throws IOException {

        this.writeLineStart();

        this.writeId(vertex.id());
        this.writeSplitter();

        this.writeValue(vertex.value());

        if (this.outputVertexAdjacentEdges) {
            this.writeSplitter();
            this.writeEdges(vertex.edges());
        }
        if (this.outputVertexProperties) {
            this.writeSplitter();
            this.writeProperties(vertex.properties());
        }
        this.writeLineEnd();
    }

    @Override
    public void writeEdges(Edges edges) throws IOException {
        this.writeArrayStart();
        int size = edges.size();
        int i = 0;
        for (Edge edge : edges) {
            this.writeEdge(edge);
            if (++i < size) {
                this.writeSplitter();
            }
        }
        this.writeArrayEnd();
    }

    @Override
    public void writeEdge(Edge edge) throws IOException {
        this.writeObjectStart();

        this.writeId(edge.targetId());
        this.writeSplitter();

        this.writeValue(edge.value());
        if (this.outputEdgeProperties) {
            this.writeSplitter();
            this.writeProperties(edge.properties());
        }
        this.writeObjectEnd();
    }

    @Override
    public void writeProperties(Properties properties) throws IOException {
        this.writeObjectStart();
        int size = properties.get().size();
        int i = 0;
        for (Map.Entry<String, Value<?>> entry : properties.get().entrySet()) {
            this.writeValue(entry.getValue());
            if (++i < size) {
                this.writeSplitter();
            }
        }
        this.writeObjectEnd();
    }

    @Override
    public void writeObjectStart() throws IOException {
        this.writeRawString("{");
    }

    @Override
    public void writeObjectEnd() throws IOException {
        this.writeRawString("}");
    }

    @Override
    public void writeArrayStart() throws IOException {
        this.writeRawString("[");
    }

    @Override
    public void writeArrayEnd() throws IOException {
        this.writeRawString("]");
    }

    @Override
    public void writeKey(String key) throws IOException {
        // pass
    }

    @Override
    public void writeJoiner() throws IOException {
        // pass
    }

    @Override
    public void writeSplitter() throws IOException {
        this.writeRawString(",");
    }
}
