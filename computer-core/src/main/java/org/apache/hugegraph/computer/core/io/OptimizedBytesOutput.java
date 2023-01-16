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

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.util.CoderUtil;
import org.apache.hugegraph.util.E;

public class OptimizedBytesOutput implements BytesOutput {

    private final UnsafeBytesOutput out;

    public OptimizedBytesOutput(int size) {
        this(new UnsafeBytesOutput(size));
    }

    public OptimizedBytesOutput(UnsafeBytesOutput out) {
        this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
        this.out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        this.out.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        this.out.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        this.out.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        this.out.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        this.out.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        this.writeVInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        this.writeVLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        this.out.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        this.out.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        this.out.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        this.out.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        this.writeString(s);
    }

    @Override
    public long position() {
        return this.out.position();
    }

    @Override
    public void seek(long position) throws IOException {
        this.out.seek(position);
    }

    @Override
    public long skip(long n) throws IOException {
        return this.out.skip(n);
    }

    @Override
    public void write(RandomAccessInput input, long offset, long length)
                      throws IOException {
        this.out.write(input, offset, length);
    }

    @Override
    public void close() throws IOException {
        this.out.close();
    }

    @Override
    public void writeFixedInt(int v) throws IOException {
        this.out.writeFixedInt(v);
    }

    @Override
    public void writeFixedInt(long position, int v) throws IOException {
        this.out.writeFixedInt(position, v);
    }

    @Override
    public byte[] buffer() {
        return this.out.buffer();
    }

    @Override
    public byte[] toByteArray() {
        return this.out.toByteArray();
    }

    private void writeVInt(int value) throws IOException {
        // NOTE: negative numbers are not compressed
        if (value > 0x0fffffff || value < 0) {
            this.writeByte(0x80 | ((value >>> 28) & 0x7f));
        }
        if (value > 0x1fffff || value < 0) {
            this.writeByte(0x80 | ((value >>> 21) & 0x7f));
        }
        if (value > 0x3fff || value < 0) {
            this.writeByte(0x80 | ((value >>> 14) & 0x7f));
        }
        if (value > 0x7f || value < 0) {
            this.writeByte(0x80 | ((value >>> 7) & 0x7f));
        }
        this.writeByte(value & 0x7f);
    }

    private void writeVLong(long value) throws IOException {
        if (value < 0) {
            this.writeByte((byte) 0x81);
        }
        if (value > 0xffffffffffffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 56) & 0x7f));
        }
        if (value > 0x1ffffffffffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 49) & 0x7f));
        }
        if (value > 0x3ffffffffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 42) & 0x7f));
        }
        if (value > 0x7ffffffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 35) & 0x7f));
        }
        if (value > 0xfffffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 28) & 0x7f));
        }
        if (value > 0x1fffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 21) & 0x7f));
        }
        if (value > 0x3fffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 14) & 0x7f));
        }
        if (value > 0x7fL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 7) & 0x7f));
        }
        this.write((int) value & 0x7f);
    }

    private void writeString(String val) throws IOException {
        this.writeBytes(CoderUtil.encode(val));
    }

    private void writeBytes(byte[] bytes) throws IOException {
        E.checkArgument(bytes.length <= Constants.UINT16_MAX,
                        "The max length of bytes is %s, but got %s",
                        Constants.UINT16_MAX, bytes.length);
        this.writeVInt(bytes.length);
        this.write(bytes);
    }
}
