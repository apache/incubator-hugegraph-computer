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
import java.nio.ByteBuffer;
import java.util.UUID;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;

public class UuidId implements Id {

    private long high;
    private long low;

    public UuidId() {
        this.high = 0L;
        this.low = 0L;
    }

    public UuidId(UUID uuid) {
        this.high = uuid.getMostSignificantBits();
        this.low = uuid.getLeastSignificantBits();
    }

    @Override
    public IdType type() {
        return IdType.UUID;
    }

    @Override
    public IdValue idValue() {
        // len = Byte.BYTES + Long.BYTES + Long.BYTES;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(17)) {
            StreamGraphOutput output = new StreamGraphOutput(baos);
            output.writeId(this);
            return new IdValue(baos.toByteArray());
        } catch (IOException e) {
            throw new ComputerException("Failed to get idValue from id '%s'",
                                        e, this);
        }
    }

    @Override
    public Object asObject() {
        return new UUID(this.high, this.low);
    }

    @Override
    public long asLong() {
        throw new ComputerException("Can't convert UuidId to long");
    }

    @Override
    public byte[] asBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(this.high);
        buffer.putLong(this.low);
        return buffer.array();
    }

    @Override
    public void read(GraphInput in) throws IOException {
        this.high = in.readLong();
        this.low = in.readLong();
    }

    @Override
    public void write(GraphOutput out) throws IOException {
        out.writeLong(this.high);
        out.writeLong(this.low);
    }

    @Override
    public int compareTo(Id obj) {
        int cmp = this.type().code() - obj.type().code();
        if (cmp != 0) {
            return cmp;
        }
        UuidId other = (UuidId) obj;
        cmp = Long.compare(this.high, other.high);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.low, other.low);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UuidId)) {
            return false;
        }
        UuidId other = (UuidId) obj;
        return this.high == other.high && this.low == other.low;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.high) ^ Long.hashCode(this.low);
    }

    @Override
    public String toString() {
        return new UUID(this.high, this.low).toString();
    }
}
