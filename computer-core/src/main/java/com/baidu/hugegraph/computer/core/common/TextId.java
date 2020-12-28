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

package com.baidu.hugegraph.computer.core.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.baidu.hugegraph.computer.core.util.ByteArrayUtil;
import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.computer.core.util.PlainByteArrayComparator;

public class TextId implements Id<TextId> {

    public static final byte [] EMPTY_BYTES = new byte[0];

    private byte[] bytes;
    private int length;

    public TextId() {
        this.bytes = EMPTY_BYTES;
    }

    public TextId(String value) {
        ByteBuffer bb = CoderUtil.encode(value);
        this.bytes = bb.array();
        this.length = bb.limit();
    }

    @Override
    public ValueType type() {
        return ValueType.TEXT_ID;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.length);
        out.write(this.bytes, 0, this.length);
    }

    @Override
    public void read(DataInput in) throws IOException {
        int len = in.readInt();
        this.ensureCapacity(len);
        in.readFully(this.bytes, 0, len);
        this.length = len;
    }

    public byte[] bytes() {
        return this.bytes;
    }

    public int length() {
        return this.length;
    }

    private void ensureCapacity(int len) {
        if (this.bytes == null || this.bytes.length < len) {
            this.bytes = new byte[len];
        }
    }

    @Override
    public int compareTo(TextId o) {
        return PlainByteArrayComparator.compare(this.bytes, this.length,
                                                o.bytes, o.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TextId)) {
            return false;
        }
        TextId other = (TextId) obj;
        return PlainByteArrayComparator.compare(this.bytes, this.length,
                                                other.bytes, other.length) == 0;
    }

    @Override
    public int hashCode() {
        return ByteArrayUtil.hashBytes(this.bytes, this.length);
    }

    @Override
    public String toString() {
        return CoderUtil.decode(this.bytes, 0, this.length);
    }
}
