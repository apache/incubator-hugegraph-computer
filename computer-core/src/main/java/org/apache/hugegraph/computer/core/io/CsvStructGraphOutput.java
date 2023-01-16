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
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;

public class CsvStructGraphOutput extends StructGraphOutput {

    public CsvStructGraphOutput(ComputerContext context,
                                StructRandomAccessOutput out) {
        super(context, out);
    }

    @Override
    public void writeVertex(Vertex vertex) throws IOException {
        this.writeLineStart();

        this.writeId(vertex.id());
        this.writeSplitter();

        this.writeValue(vertex.value());

        if (this.config.outputVertexAdjacentEdges()) {
            this.writeSplitter();
            this.writeEdges(vertex.edges());
        }
        if (this.config.outputVertexProperties()) {
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

        this.out.writeString(edge.label());
        this.writeSplitter();

        this.out.writeString(edge.name());

        if (this.config.outputEdgeProperties()) {
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
        for (Map.Entry<String, Value> entry : properties.get().entrySet()) {
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
        // pass
    }

    @Override
    public void writeJoiner() throws IOException {
        // pass
    }

    @Override
    public void writeSplitter() throws IOException {
        this.out.writeRawString(",");
    }
}
