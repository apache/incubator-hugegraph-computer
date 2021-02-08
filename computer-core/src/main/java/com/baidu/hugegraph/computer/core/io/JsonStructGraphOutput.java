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

public class JsonStructGraphOutput extends StructGraphOutput {

    public JsonStructGraphOutput(DataOutput out) {
        super(out);
    }

    @Override
    public void writeVertex(Vertex vertex) throws IOException {
        ComputerContext context = ComputerContext.instance();

        this.writeLineStart();
        this.writeObjectStart();

        this.writeKey("id");
        this.writeJoiner();
        this.writeId(vertex.id());
        this.writeSplitter();

        String valueName = context.config().vertexValueName();
        this.writeKey(valueName);
        this.writeJoiner();
        this.writeValue(vertex.value());

        if (context.config().outputVertexAdjacentEdges()) {
            this.writeSplitter();
            this.writeKey("adjacent_edges");
            this.writeJoiner();
            this.writeEdges(vertex.edges());
        }
        if (context.config().outputVertexProperties()) {
            this.writeSplitter();
            this.writeKey("properties");
            this.writeJoiner();
            this.writeProperties(vertex.properties());
        }
        this.writeObjectEnd();
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
        ComputerContext context = ComputerContext.instance();

        this.writeObjectStart();

        this.writeKey("target_id");
        this.writeJoiner();
        this.writeId(edge.targetId());
        this.writeSplitter();

        String valueName = context.config().edgeValueName();
        this.writeKey(valueName);
        this.writeJoiner();
        this.writeValue(edge.value());
        if (context.config().outputEdgeProperties()) {
            this.writeSplitter();
            this.writeKey("properties");
            this.writeJoiner();
            this.writeProperties(edge.properties());
        }
        this.writeObjectEnd();
    }

    @Override
    public void writeProperties(Properties properties) throws IOException {
        this.writeObjectStart();
        int size = properties.get().size();
        int i = 0;
        for (Map.Entry<String, Value> entry : properties.get().entrySet()) {
            this.writeKey(entry.getKey());
            this.writeJoiner();
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
        this.writeString(key);
    }

    @Override
    public void writeJoiner() throws IOException {
        this.writeRawString(":");
    }

    @Override
    public void writeSplitter() throws IOException {
        this.writeRawString(",");
    }
}
