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

import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class OptimizedUnsafeBytesInput extends UnsafeBytesInput {

    public OptimizedUnsafeBytesInput(byte[] buffer) {
        this(buffer, buffer.length);
    }

    public OptimizedUnsafeBytesInput(byte[] buffer, int limit) {
        this(buffer, 0, limit);
    }

    public OptimizedUnsafeBytesInput(byte[] buffer, long limit) {
        this(buffer, 0, (int) limit);
    }

    public OptimizedUnsafeBytesInput(byte[] buffer, int position, int limit) {
        super(buffer, position, limit);
    }

    @Override
    public OptimizedUnsafeBytesInput duplicate() throws IOException {
        return new OptimizedUnsafeBytesInput(this.buffer(),
                                             (int) this.position(),
                                             this.limit());
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
    public String readUTF() throws IOException {
        return this.readString();
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
