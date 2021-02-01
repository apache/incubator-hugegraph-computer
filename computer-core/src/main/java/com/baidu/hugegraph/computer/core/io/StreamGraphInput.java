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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.baidu.hugegraph.computer.core.allocator.RecyclerReference;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdges;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdFactory;
import com.baidu.hugegraph.computer.core.graph.properties.DefaultProperties;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactory;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class StreamGraphInput implements GraphInput {

    private final DataInputStream in;

    public StreamGraphInput(InputStream in) {
        this(new DataInputStream(in));
    }

    public StreamGraphInput(DataInputStream in) {
        this.in = in;
    }

    @Override
    public Vertex readVertex() throws IOException {
        ComputerContext context = ComputerContext.instance();

        Id id = this.readId();
        Value value = this.readValue();
        RecyclerReference<Vertex> reference = context.allocator().newVertex();
        try {
            Vertex vertex = reference.get();
            vertex.id(id);
            vertex.value(value);

            if (context.config().outputVertexAdjacentEdges()) {
                Edges edges = this.readOutEdges();
                vertex.edges(edges);
            }
            if (context.config().outputVertexProperties()) {
                Properties properties = this.readProperties();
                vertex.properties(properties);
            }
            return vertex;
        } catch (Exception e) {
            context.allocator().freeVertex(reference);
            throw new ComputerException("Failed to read vertex", e);
        }
    }

    @Override
    public Edges readOutEdges() throws IOException {
        int numEdges = this.readInt();
        Edges edges = new DefaultEdges(numEdges);
        for (int i = 0; i < numEdges; ++i) {
            Edge edge = this.readEdge();
            edges.add(edge);
        }
        return edges;
    }

    @Override
    public Edge readEdge() throws IOException {
        ComputerContext context = ComputerContext.instance();
        // Write necessary
        Id targetId = this.readId();
        Value value = this.readValue();
        RecyclerReference<Edge> reference = context.allocator().newEdge();
        try {
            Edge edge = reference.get();
            edge.targetId(targetId);
            edge.value(value);

            if (context.config().outputEdgeProperties()) {
                Properties properties = this.readProperties();
                edge.properties(properties);
            }
            return edge;
        } catch (Exception e) {
            context.allocator().freeEdge(reference);
            throw new ComputerException("Failed to read edge", e);
        }
    }

    @Override
    public Properties readProperties() throws IOException {
        Properties properties = new DefaultProperties();
        int size = this.readInt();
        for (int i = 0; i < size; i++) {
            String key = this.readString();
            Value value = this.readValue();
            properties.put(key, value);
        }
        return properties;
    }

    @Override
    public Id readId() throws IOException {
        byte type = this.readByte();
        Id id = IdFactory.createID(type);
        id.read(this);
        return id;
    }

    @Override
    public Value readValue() throws IOException {
        ComputerContext context = ComputerContext.instance();
        ValueType valueType = context.config().valueType();
        Value value = ValueFactory.createValue(valueType);
        value.read(this);
        return value;
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
        return this.readByte() & 0x000000ff;
    }

    public int readUInt16() throws IOException {
        return this.readShort() & 0x0000ffff;
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

    @Override
    public long readLong() throws IOException {
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
}
