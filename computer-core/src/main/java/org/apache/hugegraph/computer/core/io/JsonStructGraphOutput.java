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

package org.apache.hugegraph.computer.core.io;

import java.io.IOException;
import java.util.Map;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;

public class JsonStructGraphOutput extends StructGraphOutput {

    private final String valueName;
    private final boolean outputEdges;
    private final boolean outputVertexProperties;
    private final boolean outputEdgeProperties;

    public JsonStructGraphOutput(ComputerContext context,
                                 StructRandomAccessOutput out) {
        super(context, out);
        this.valueName = this.config.get(ComputerOptions.OUTPUT_RESULT_NAME);
        this.outputEdges = this.config.outputVertexAdjacentEdges();
        this.outputVertexProperties = this.config.outputVertexProperties();
        this.outputEdgeProperties = this.config.outputEdgeProperties();
    }

    @Override
    public void writeVertex(Vertex vertex) throws IOException {
        this.writeLineStart();
        this.writeObjectStart();

        this.writeKey("id");
        this.writeJoiner();
        this.writeId(vertex.id());
        this.writeSplitter();

        this.writeKey(this.valueName);
        this.writeJoiner();
        this.writeValue(vertex.value());

        if (this.outputEdges) {
            this.writeSplitter();
            this.writeKey("adjacent_edges");
            this.writeJoiner();
            this.writeEdges(vertex.edges());
        }
        if (this.outputVertexProperties) {
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
        this.writeObjectStart();

        this.writeKey("target_id");
        this.writeJoiner();
        this.writeId(edge.targetId());
        this.writeSplitter();

        this.writeKey("label");
        this.writeJoiner();
        this.out.writeString(edge.label());
        this.writeSplitter();

        this.writeKey("name");
        this.writeJoiner();
        this.out.writeString(edge.name());

        if (this.outputEdgeProperties) {
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
        this.out.writeRawString("{");
    }

    @Override
    public void writeObjectEnd() throws IOException {
        this.out.writeRawString("}");
    }

    @Override
    public void writeArrayStart() throws IOException {
        this.out.writeRawString("[");
    }

    @Override
    public void writeArrayEnd() throws IOException {
        this.out.writeRawString("]");
    }

    @Override
    public void writeKey(String key) throws IOException {
        this.out.writeString(key);
    }

    @Override
    public void writeJoiner() throws IOException {
        this.out.writeRawString(":");
    }

    @Override
    public void writeSplitter() throws IOException {
        this.out.writeRawString(",");
    }
}
