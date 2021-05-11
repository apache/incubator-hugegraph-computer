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

import java.io.IOException;

import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.util.IdValueUtil;
import com.baidu.hugegraph.util.NumericUtil;

public class LongId implements Id {

    private long id;

    public LongId() {
        this.id = 0L;
    }

    public LongId(long value) {
        this.id = value;
    }

    @Override
    public IdType type() {
        return IdType.LONG;
    }

    @Override
    public IdValue idValue() {
        // len = id type(1 byte) + long data(1 ~ 10 bytes)
        return IdValueUtil.toIdValue(this, 11);
    }

    @Override
    public Object asObject() {
        return this.id;
    }

    @Override
    public long asLong() {
        return this.id;
    }

    @Override
    public byte[] asBytes() {
        return NumericUtil.longToBytes(this.id);
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.id = in.readLong();
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeLong(this.id);
    }

    @Override
    public int compareTo(Id obj) {
        int cmp = this.type().code() - obj.type().code();
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.id, obj.asLong());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LongId)) {
            return false;
        }
        return this.id == ((LongId) obj).id;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.id);
    }

    @Override
    public String toString() {
        return String.valueOf(this.id);
    }
}
