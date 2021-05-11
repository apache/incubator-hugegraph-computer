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

import static com.baidu.hugegraph.computer.core.common.Constants.UINT16_MAX;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.util.E;

public class OptimizedUnsafeBytesOutput extends UnsafeBytesOutput {

    public OptimizedUnsafeBytesOutput() {
        super();
    }

    public OptimizedUnsafeBytesOutput(int size) {
        super(size);
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
    public void writeUTF(String s) throws IOException {
        this.writeString(s);
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
        E.checkArgument(bytes.length <= UINT16_MAX,
                        "The max length of bytes is %s, but got %s",
                        UINT16_MAX, bytes.length);
        this.writeVInt(bytes.length);
        this.write(bytes);
    }
}
