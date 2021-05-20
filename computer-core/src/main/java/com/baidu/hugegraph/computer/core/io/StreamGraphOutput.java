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

import java.io.IOException;
import java.util.Map;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public class StreamGraphOutput implements GraphOutput {

    private final RandomAccessOutput out;
    protected final Config config;

    public StreamGraphOutput(ComputerContext context, RandomAccessOutput out) {
        this.config = context.config();
        this.out = out;
    }

    @Override
    public void writeVertex(Vertex vertex) throws IOException {
        // Write necessary
        this.writeId(vertex.id());
        this.writeValue(vertex.value());

        if (this.config.outputVertexAdjacentEdges()) {
            this.writeEdges(vertex.edges());
        }
        if (this.config.outputVertexProperties()) {
            this.writeProperties(vertex.properties());
        }
    }

    @Override
    public void writeEdges(Edges edges) throws IOException {
        int size = edges.size();
        this.out.writeInt(size);
        if (size == 0) {
            return;
        }
        long startPosition = this.out.writeIntLength(0);
        for (Edge edge : edges) {
            this.writeEdge(edge);
        }
        long endPosition = this.out.position();
        long length = endPosition - startPosition - Constants.INT_LEN;
        this.out.writeIntLength(startPosition, (int) length);
    }

    @Override
    public void writeEdge(Edge edge) throws IOException {
        // Write necessary
        this.writeId(edge.targetId());
        if (edge.label() != null) {
            this.out.writeBoolean(true);
            this.out.writeUTF(edge.label());
        } else {
            this.out.writeBoolean(false);
        }
        if (edge.name() != null) {
            this.out.writeBoolean(true);
            this.out.writeUTF(edge.name());
        } else {
            this.out.writeBoolean(false);
        }
        if (this.config.outputEdgeProperties()) {
            this.writeProperties(edge.properties());
        }
    }

    @Override
    public void writeProperties(Properties properties) throws IOException {
        Map<String, Value<?>> keyValues = properties.get();
        this.out.writeInt(keyValues.size());
        for (Map.Entry<String, Value<?>> entry : keyValues.entrySet()) {
            this.out.writeUTF(entry.getKey());
            Value<?> value = entry.getValue();
            this.out.writeByte(value.type().code());
            value.write(this.out);
        }
    }

    @Override
    public void writeId(Id id) throws IOException {
        this.out.writeByte(id.type().code());
        id.write(this.out);
    }

    @Override
    public void writeValue(Value<?> value) throws IOException {
        value.write(this.out);
    }

    @Override
    public void close() throws IOException {
        this.out.close();
    }
}
