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
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import com.baidu.hugegraph.computer.core.common.SerialEnum;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.io.BytesInput;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.util.BytesUtil;
import com.baidu.hugegraph.computer.core.util.CoderUtil;
import com.baidu.hugegraph.computer.core.util.IdValueUtil;
import com.baidu.hugegraph.util.E;

public class BytesId implements Id {

    private IdType type;
    private byte[] bytes;
    private int length;

    public BytesId(IdType type, byte[] bytes) {
        this(type, bytes, bytes.length);
    }

    public BytesId(IdType type, byte[] bytes, long length) {
        E.checkArgument(bytes != null, "The bytes can't be null");
        this.type = type;
        this.bytes = bytes;
        this.length = (int) length;
    }

    public static BytesId of() {
        return BytesId.of(0L);
    }

    public static BytesId of(long value) {
        BytesOutput output = IOFactory.createBytesOutput(9);
        try {
            output.writeLong(value);
        } catch (IOException e) {
            throw new ComputerException("Failed to write Long object to " +
                                        "BytesId");
        }
        return new BytesId(IdType.LONG, output.buffer(), output.position());
    }

    public static BytesId of(String value) {
        E.checkArgument(value != null, "The value can't be null");
        return new BytesId(IdType.UTF8, CoderUtil.encode(value));
    }

    public static BytesId of(UUID value) {
        E.checkArgument(value != null, "The value can't be null");
        long high = value.getMostSignificantBits();
        long low = value.getLeastSignificantBits();
        BytesOutput output = IOFactory.createBytesOutput(18);
        try {
            output.writeLong(high);
            output.writeLong(low);
        } catch (IOException e) {
            throw new ComputerException("Failed to write UUID object to " +
                                        "BytesId");
        }
        return new BytesId(IdType.UUID, output.buffer(), output.position());
    }

    private byte[] truncate() {
        if (this.bytes.length == this.length) {
            return this.bytes;
        } else {
            return Arrays.copyOfRange(this.bytes, 0, this.length);
        }
    }

    @Override
    public IdType type() {
        return this.type;
    }

    @Override
    public int length() {
        return this.length;
    }

    @Override
    public IdValue idValue() {
        int len = Byte.BYTES + Integer.BYTES + this.length;
        return IdValueUtil.toIdValue(this, len);
    }

    @Override
    public Object asObject() {
        byte[] bytes = this.truncate();
        switch (this.type) {
            case LONG:
                BytesInput input = IOFactory.createBytesInput(bytes);
                try {
                    return input.readLong();
                } catch (IOException e) {
                    throw new ComputerException("Failed to read BytesId to " +
                                                "Long object");
                }
            case UTF8:
                return CoderUtil.decode(bytes);
            case UUID:
                input = IOFactory.createBytesInput(bytes);
                try {
                    long high = input.readLong();
                    long low = input.readLong();
                    return new UUID(high, low);
                } catch (IOException e) {
                    throw new ComputerException("Failed to read BytesId to " +
                                                "UUID object");
                }
            default:
                throw new ComputerException("Unexpected IdType %s", this.type);
        }
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.type = SerialEnum.fromCode(IdType.class, in.readByte());
        int len = in.readInt();
        this.bytes = BytesUtil.ensureCapacityWithoutCopy(this.bytes, len);
        in.readFully(this.bytes, 0, len);
        this.length = len;
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeByte(this.type.code());
        out.writeInt(this.length);
        out.write(this.bytes, 0, this.length);
    }

    @Override
    public int compareTo(Id obj) {
        int cmp = this.type().code() - obj.type().code();
        if (cmp != 0) {
            return cmp;
        }
        BytesId other = (BytesId) obj;
        return BytesUtil.compare(this.bytes, this.length,
                                 other.bytes, other.length);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BytesId)) {
            return false;
        }
        BytesId other = (BytesId) obj;
        if (this.type != other.type) {
            return false;
        }
        return BytesUtil.compare(this.bytes, this.length,
                                 other.bytes, other.length) == 0;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(this.type);
        result = 31 * result + BytesUtil.hashBytes(this.bytes, this.length);
        return result;
    }

    @Override
    public String toString() {
        return this.asObject().toString();
    }
}