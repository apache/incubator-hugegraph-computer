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

package com.baidu.hugegraph.computer.core.graph.id;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.computer.core.io.OptimizedStreamGraphOutput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.computer.core.util.ByteArrayUtil;
import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.util.E;

public class Utf8Id implements Id {

    private byte[] bytes;
    private int length;

    public Utf8Id() {
        this.bytes = Constants.EMPTY_BYTES;
        this.length = 0;
    }

    public Utf8Id(String value) {
        E.checkArgument(value != null, "Value can't be null");
        this.bytes = CoderUtil.encode(value);
        this.length = bytes.length;
    }

    public byte[] bytes() {
        return this.bytes;
    }

    public int length() {
        return this.length;
    }

    @Override
    public IdType type() {
        return IdType.UTF8;
    }

    @Override
    public IdValue idValue() {
        int len = Byte.BYTES + Integer.BYTES + this.length;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(len)) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(baos);
            output.writeId(this);
            return new IdValue(baos.toByteArray());
        } catch (IOException e) {
            throw new ComputerException("Failed to get idValue from id '%s'",
                                        e, this);
        }
    }

    @Override
    public Object asObject() {
        return CoderUtil.decode(this.bytes, 0, this.length);
    }

    @Override
    public long asLong() {
        return Long.parseLong(CoderUtil.decode(this.bytes, 0, this.length));
    }

    @Override
    public byte[] asBytes() {
        if (this.bytes.length == this.length) {
            return this.bytes;
        } else {
            return Arrays.copyOfRange(this.bytes, 0, this.length);
        }
    }

    @Override
    public void read(GraphInput in) throws IOException {
        int len = in.readInt();
        this.bytes = ByteArrayUtil.ensureCapacityWithoutCopy(this.bytes, len);
        in.readFully(this.bytes, 0, len);
        this.length = len;
    }

    @Override
    public void write(GraphOutput out) throws IOException {
        out.writeInt(this.length);
        out.write(this.bytes, 0, this.length);
    }

    @Override
    public int compareTo(Id obj) {
        int cmp = this.type().code() - obj.type().code();
        if (cmp != 0) {
            return cmp;
        }
        Utf8Id other = (Utf8Id) obj;
        return ByteArrayUtil.compare(this.bytes, this.length,
                                     other.bytes, other.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Utf8Id)) {
            return false;
        }
        Utf8Id other = (Utf8Id) obj;
        return ByteArrayUtil.compare(this.bytes, this.length,
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
