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

import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.computer.core.util.BytesUtil;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class IdValue implements Value<IdValue> {

    public static final byte[] EMPTY_BYTES = new byte[0];

    private byte[] bytes;
    private int length;

    public IdValue() {
        this.bytes = EMPTY_BYTES;
        this.length = 0;
    }

    public IdValue(byte[] bytes) {
        this.bytes = bytes;
        this.length = bytes.length;
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
    public void read(GraphInput in) throws IOException {
        int len = in.readInt();
        this.bytes = BytesUtil.ensureCapacityWithoutCopy(this.bytes, len);
        in.readFully(this.bytes, 0, len);
        this.length = len;
    }

    @Override
    public void write(GraphOutput out) throws IOException {
        out.writeInt(this.length);
        out.write(this.bytes, 0, this.length);
    }

    public void writeId(GraphOutput out) throws IOException {
        out.write(this.bytes, 0, this.length);
    }

    @Override
    public int compareTo(IdValue obj) {
        E.checkArgumentNotNull(obj, "The compare argument can't be null");
        return BytesUtil.compare(this.bytes, this.length,
                                 obj.bytes, obj.length);
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
