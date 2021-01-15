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

import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdFactory;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactory;
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
    public Id readId() throws IOException {
        byte type = this.readByte();
        Id id = IdFactory.createID(type);
        id.read(this);
        return id;
    }

    @Override
    public Value readValue() throws IOException {
        byte typeCode = this.readByte();
        Value value = ValueFactory.createValue(typeCode);
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
