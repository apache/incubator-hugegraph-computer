/*
 *
 *  Copyright 2017 HugeGraph Authors
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with this
 *  work for additional information regarding copyright ownership. The ASF
 *  licenses this file to You under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package com.baidu.hugegraph.computer.core.graph.value;

import java.io.IOException;
import java.util.Arrays;

import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.util.BytesUtil;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class IdValue implements Value<IdValue> {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private byte[] bytes;
    private int length;

    public IdValue() {
        this(EMPTY_BYTES, 0);
    }

    public IdValue(byte[] bytes) {
        this(bytes, bytes.length);
    }

    public IdValue(byte[] bytes, int length) {
        this.bytes = bytes;
        this.length = length;
    }

    public byte[] bytes() {
        if (this.length == this.bytes.length) {
            return this.bytes;
        } else {
            return Arrays.copyOf(this.bytes, this.length);
        }
    }

    @Override
    public ValueType type() {
        return ValueType.ID_VALUE;
    }

    @Override
    public void assign(Value<IdValue> other) {
        E.checkArgument(other instanceof IdValue,
                        "Can't assign '%s'(%s) to IdValue",
                        other, other.getClass().getSimpleName());
        this.bytes = ((IdValue) other).bytes;
        this.length = ((IdValue) other).length;
    }

    @Override
    public IdValue copy() {
        return new IdValue(this.bytes, this.length);
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        int len = in.readInt();
        this.bytes = BytesUtil.ensureCapacityWithoutCopy(this.bytes, len);
        in.readFully(this.bytes, 0, len);
        this.length = len;
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.length);
        out.write(this.bytes, 0, this.length);
    }

    @Override
    public int compareTo(IdValue other) {
        E.checkArgumentNotNull(other, "The compare argument can't be null");
        return BytesUtil.compare(this.bytes, this.length,
                                 other.bytes, other.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IdValue)) {
            return false;
        }
        IdValue other = (IdValue) obj;
        return BytesUtil.compare(this.bytes, this.length,
                                 other.bytes, other.length) == 0;
    }

    @Override
    public int hashCode() {
        return BytesUtil.hashBytes(this.bytes, this.length);
    }

    @Override
    public String toString() {
        return Bytes.toHex(this.bytes());
    }
}
