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
        long startPosition = this.writeFullInt(0);
        for (Edge edge : edges) {
            this.writeEdge(edge);
        }
        long endPosition = this.out.position();
        long length = endPosition - startPosition - Constants.INT_LEN;
        this.writeFullInt(startPosition, (int) length);
    }

    @Override
    public void writeEdge(Edge edge) throws IOException {
        // TODO: try to reduce call ComputerContext.instance() directly.
        ComputerContext context = ComputerContext.instance();
        // Write necessary
        this.writeId(edge.targetId());
        this.writeValue(edge.value());

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
            this.writeValue(entry.getValue());
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

    protected final long writeFullInt(int v) throws IOException {
        long position = this.out.position();
        this.out.writeInt(v);
        return position;
    }

    protected final void writeFullInt(long position, int v) throws IOException {
        this.out.writeInt(position, v);
    }

    protected final long writeFullLong(long v) throws IOException {
        long position = this.out.position();
        this.out.writeLong(v);
        return position;
    }

    @Override
    public void close() throws IOException {
        this.out.close();
    }

    @Override
    public void close() throws IOException {
        this.out.close();
    }
}
