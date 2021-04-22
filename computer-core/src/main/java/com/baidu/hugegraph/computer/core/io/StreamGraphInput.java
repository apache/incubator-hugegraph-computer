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
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdFactory;
import com.baidu.hugegraph.computer.core.graph.properties.DefaultProperties;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class StreamGraphInput implements GraphInput {

    private final RandomAccessInput in;

    public StreamGraphInput(RandomAccessInput in) {
        this.in = in;
    }

    @Override
    public Vertex readVertex() throws IOException {
        ComputerContext context = ComputerContext.instance();
        GraphFactory factory = ComputerContext.instance().graphFactory();

        Id id = this.readId();
        Value<?> value = this.readValue();
        /*
         * TODO: Reuse Vertex has two ways
         * 1. ObjectPool(Recycler), need consider safely free object
         * 2. Precreate Vertex Object outside then fill fields here
         */
        Vertex vertex = factory.createVertex(id, value);

        if (context.config().outputVertexAdjacentEdges()) {
            Edges edges = this.readEdges();
            vertex.edges(edges);
        }
        if (context.config().outputVertexProperties()) {
            Properties properties = this.readProperties();
            vertex.properties(properties);
        }
        return vertex;
    }

    @Override
    public Edges readEdges() throws IOException {
        // TODO: When the previous vertex is super vertex and has a few of
        //  edges fragment. If the super vertex not read all the fragment,
        //  the current vertex may read the super vertex's edges.
        ComputerContext context = ComputerContext.instance();
        GraphFactory factory = context.graphFactory();

        int numEdges = this.readInt();
        if (numEdges == 0) {
            return factory.createEdges(0);
        }
        @SuppressWarnings("unused")
        int bytes = this.readFullInt();
        Edges edges = factory.createEdges(numEdges);
        // TODO: lazy deserialization
        for (int i = 0; i < numEdges; ++i) {
            Edge edge = this.readEdge();
            edges.add(edge);
        }
        return edges;
    }

    @Override
    public Edge readEdge() throws IOException {
        ComputerContext context = ComputerContext.instance();
        GraphFactory factory = ComputerContext.instance().graphFactory();

        // Write necessary
        Id targetId = this.readId();
        Value<?> value = this.readValue();
        Edge edge = factory.createEdge(targetId, value);

        if (context.config().outputEdgeProperties()) {
            Properties properties = this.readProperties();
            edge.properties(properties);
        }
        return edge;
    }

    @Override
    public Properties readProperties() throws IOException {
        Properties properties = new DefaultProperties();
        int size = this.readInt();
        for (int i = 0; i < size; i++) {
            String key = this.readString();
            Value<?> value = this.readValue();
            properties.put(key, value);
        }
        return properties;
    }

    @Override
    public Id readId() throws IOException {
        byte type = this.readByte();
        Id id = IdFactory.createId(type);
        id.read(this);
        return id;
    }

    @Override
    public Value<?> readValue() throws IOException {
        ComputerContext context = ComputerContext.instance();
        ValueType valueType = context.config().valueType();
        Value<?> value = context.valueFactory().createValue(valueType);
        value.read(this);
        return value;
    }

    public long position() {
        return this.in.position();
    }

    public void seek(long position) throws IOException {
        this.in.seek(position);
    }

    public long skip(long n) throws IOException {
        return this.in.skip(n);
    }

    public int readVInt() throws IOException {
        byte leading = this.readByte();
        E.checkArgument(leading != 0x80,
                        "Unexpected varint with leading byte '0x%s'",
                        Bytes.toHex(leading));
        int value = leading & 0x7f;
        if (leading >= 0) {
            assert (leading & 0x80) == 0;
            return value;
        }

        int i = 1;
        for (; i < 5; i++) {
            byte b = this.readByte();
            if (b >= 0) {
                value = b | (value << 7);
                break;
            } else {
                value = (b & 0x7f) | (value << 7);
            }
        }

        E.checkArgument(i < 5,
                        "Unexpected varint %s with too many bytes(%s)",
                        value, i + 1);
        E.checkArgument(i < 4 || (leading & 0x70) == 0,
                        "Unexpected varint %s with leading byte '0x%s'",
                        value, Bytes.toHex(leading));
        return value;
    }

    public long readVLong() throws IOException {
        byte leading = this.readByte();
        E.checkArgument(leading != 0x80,
                        "Unexpected varlong with leading byte '0x%s'",
                        Bytes.toHex(leading));
        long value = leading & 0x7fL;
        if (leading >= 0) {
            assert (leading & 0x80) == 0;
            return value;
        }

        int i = 1;
        for (; i < 10; i++) {
            byte b = this.readByte();
            if (b >= 0) {
                value = b | (value << 7);
                break;
            } else {
                value = (b & 0x7f) | (value << 7);
            }
        }

        E.checkArgument(i < 10,
                        "Unexpected varlong %s with too many bytes(%s)",
                        value, i + 1);
        E.checkArgument(i < 9 || (leading & 0x7e) == 0,
                        "Unexpected varlong %s with leading byte '0x%s'",
                        value, Bytes.toHex(leading));
        return value;
    }

    public int readUInt8() throws IOException {
        return this.readUnsignedByte();
    }

    public int readUInt16() throws IOException {
        return this.readUnsignedShort();
    }

    public long readUInt32() throws IOException {
        return this.readInt() & 0xffffffffL;
    }

    public String readString() throws IOException {
        return CoderUtil.decode(this.readBytes());
    }

    public byte[] readBytes() throws IOException {
        int length = this.readVInt();
        assert length >= 0;
        byte[] bytes = new byte[length];
        this.readFully(bytes, 0, length);
        return bytes;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        this.in.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        this.in.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return this.in.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return this.in.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return this.in.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return this.in.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return this.in.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return this.in.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return this.in.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return this.in.readInt();
    }

    public final int readFullInt() throws IOException {
        return this.in.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return this.in.readLong();
    }

    public final long readFullLong() throws IOException {
        return this.in.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return this.in.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return this.in.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return this.in.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return this.in.readUTF();
    }

    @Override
    public void close() throws IOException {
        this.in.close();
    }
}
