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

import org.apache.hugegraph.computer.core.util.CoderUtil;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;

public class OptimizedBytesInput implements BytesInput {

    private final UnsafeBytesInput in;

    public OptimizedBytesInput(byte[] buffer) {
        this(buffer, buffer.length);
    }

    public OptimizedBytesInput(byte[] buffer, int limit) {
        this(buffer, 0, limit);
    }

    public OptimizedBytesInput(byte[] buffer, int position, int limit) {
        this(new UnsafeBytesInput(buffer, position, limit));
    }

    public OptimizedBytesInput(UnsafeBytesInput in) {
        this.in = in;
    }

    @Override
    public long position() {
        return this.in.position();
    }

    @Override
    public void seek(long position) throws IOException {
        this.in.seek(position);
    }

    @Override
    public long skip(long n) throws IOException {
        return this.in.skip(n);
    }

    @Override
    public long available() throws IOException {
        return this.in.available();
    }

    @Override
    public OptimizedBytesInput duplicate() throws IOException {
        return new OptimizedBytesInput(this.in.duplicate());
    }

    @Override
    public int compare(long offset, long length, RandomAccessInput other,
                       long otherOffset, long otherLength) throws IOException {
        return this.in.compare(offset, length, other, otherOffset, otherLength);
    }

    @Override
    public void close() throws IOException {
        this.in.close();
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
        return this.readVInt();
    }

    @Override
    public long readLong() throws IOException {
        return this.readVLong();
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
        return this.readString();
    }

    @Override
    public int readFixedInt() throws IOException {
        return this.in.readFixedInt();
    }

    private int readVInt() throws IOException {
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

    private long readVLong() throws IOException {
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

    private String readString() throws IOException {
        return CoderUtil.decode(this.readBytes());
    }

    private byte[] readBytes() throws IOException {
        int length = this.readVInt();
        assert length >= 0;
        byte[] bytes = new byte[length];
        this.readFully(bytes, 0, length);
        return bytes;
    }
}
