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

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdFactory;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactory;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public class StreamGraphInput implements GraphInput {

    private final RandomAccessInput in;
    protected final Config config;
    protected final GraphFactory graphFactory;
    protected final ValueFactory valueFactory;

    public StreamGraphInput(ComputerContext context, RandomAccessInput in) {
        this.config = context.config();
        this.graphFactory = context.graphFactory();
        this.valueFactory = context.valueFactory();
        this.in = in;
    }

    @Override
    public Vertex readVertex() throws IOException {
        Id id = this.readId();
        Value<?> value = this.readValue();
        /*
         * TODO: Reuse Vertex has two ways
         * 1. ObjectPool(Recycler), need consider safely free object
         * 2. Precreate Vertex Object outside then fill fields here
         */
        Vertex vertex = this.graphFactory.createVertex(id, value);

        if (this.config.outputVertexAdjacentEdges()) {
            Edges edges = this.readEdges();
            vertex.edges(edges);
        }
        if (this.config.outputVertexProperties()) {
            Properties properties = this.readProperties();
            vertex.properties(properties);
        }
        return vertex;
    }

    @Override
    public Edges readEdges() throws IOException {
        /*
         * TODO: When the previous vertex is super vertex and has a few of
         *  edges fragment. If the super vertex not read all the fragment,
         *  the current vertex may read the super vertex's edges.
         */

        int numEdges = this.in.readInt();
        if (numEdges == 0) {
            return this.graphFactory.createEdges(0);
        }
        @SuppressWarnings("unused")
        int bytes = this.readFullInt();
        Edges edges = this.graphFactory.createEdges(numEdges);
        // TODO: lazy deserialization
        for (int i = 0; i < numEdges; ++i) {
            Edge edge = this.readEdge();
            edges.add(edge);
        }
        return edges;
    }

    @Override
    public Edge readEdge() throws IOException {
        // Write necessary
        Id targetId = this.readId();
        Value<?> value = this.readValue();
        Edge edge = this.graphFactory.createEdge(targetId, value);

        if (this.config.outputEdgeProperties()) {
            Properties properties = this.readProperties();
            edge.properties(properties);
        }
        return edge;
    }

    @Override
    public Properties readProperties() throws IOException {
        Properties properties = this.graphFactory.createProperties();
        int size = this.in.readInt();
        for (int i = 0; i < size; i++) {
            String key = this.in.readUTF();
            Value<?> value = this.readValue();
            properties.put(key, value);
        }
        return properties;
    }

    @Override
    public Id readId() throws IOException {
        byte type = this.in.readByte();
        Id id = IdFactory.createId(type);
        id.read(this.in);
        return id;
    }

    @Override
    public Value<?> readValue() throws IOException {
        ValueType valueType = this.config.valueType();
        Value<?> value = this.valueFactory.createValue(valueType);
        value.read(this.in);
        return value;
    }

    protected final int readFullInt() throws IOException {
        return this.in.readInt();
    }

    protected final long readFullLong() throws IOException {
        return this.in.readLong();
    }

    @Override
    public void close() throws IOException {
        this.in.close();
    }
}
