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

package org.apache.hugegraph.computer.core.graph.id;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.SerialEnum;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.io.BytesInput;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.util.BytesUtil;
import org.apache.hugegraph.computer.core.util.CoderUtil;
import org.apache.hugegraph.util.E;

public class BytesId implements Id {

    public static final BytesId EMPTY = BytesId.of(Constants.EMPTY_STR);

    private IdType idType;
    private byte[] bytes;
    private int length;

    public BytesId() {
        this.idType = EMPTY.idType;
        this.bytes = EMPTY.bytes;
        this.length = EMPTY.length;
    }

    public BytesId(IdType idType, byte[] bytes) {
        this(idType, bytes, bytes.length);
    }

    public BytesId(IdType idType, byte[] bytes, long length) {
        E.checkArgument(bytes != null, "The bytes can't be null");
        this.idType = idType;
        this.bytes = bytes;
        this.length = (int) length;
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
        BytesOutput output = IOFactory.createBytesOutput(16);
        try {
            output.writeLong(high);
            output.writeLong(low);
        } catch (IOException e) {
            throw new ComputerException("Failed to write UUID object to " +
                                        "BytesId");
        }
        return new BytesId(IdType.UUID, output.buffer(), output.position());
    }

    @Override
    public ValueType valueType() {
        return ValueType.ID;
    }

    @Override
    public IdType idType() {
        return this.idType;
    }

    @Override
    public void assign(Value other) {
        this.checkAssign(other);
        this.idType = ((BytesId) other).idType;
        this.bytes = ((BytesId) other).bytes;
        this.length = ((BytesId) other).length;
    }

    @Override
    public Id copy() {
        byte[] copyBytes = Arrays.copyOf(this.bytes, this.length);
        return new BytesId(this.idType, copyBytes, this.length);
    }

    @Override
    public int length() {
        return this.length;
    }

    @Override
    public Object value() {
        return this.asObject();
    }

    @Override
    public Object asObject() {
        switch (this.idType) {
            case LONG:
                BytesInput input = IOFactory.createBytesInput(this.bytes, 0,
                                                              this.length);
                try {
                    return input.readLong();
                } catch (IOException e) {
                    throw new ComputerException("Failed to read BytesId to " +
                                                "Long object");
                }
            case UTF8:
                return CoderUtil.decode(this.bytes, 0, this.length);
            case UUID:
                input = IOFactory.createBytesInput(this.bytes, 0, this.length);
                try {
                    long high = input.readLong();
                    long low = input.readLong();
                    return new UUID(high, low);
                } catch (IOException e) {
                    throw new ComputerException("Failed to read BytesId to " +
                                                "UUID object");
                }
            default:
                throw new ComputerException("Unexpected IdType %s",
                                            this.idType);
        }
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        assert this != EMPTY : "can't read to EMPTY id";
        this.idType = SerialEnum.fromCode(IdType.class, in.readByte());
        int len = in.readInt();
        this.bytes = BytesUtil.ensureCapacityWithoutCopy(this.bytes, len);
        in.readFully(this.bytes, 0, len);
        this.length = len;
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeByte(this.idType.code());
        out.writeInt(this.length);
        out.write(this.bytes, 0, this.length);
    }

    @Override
    public int compareTo(Value obj) {
        int typeDiff = this.valueType().compareTo(obj.valueType());
        if (typeDiff != 0) {
            return typeDiff;
        }
        typeDiff = this.idType().code() - ((Id) obj).idType().code();
        if (typeDiff != 0) {
            return typeDiff;
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
        if (this.idType != other.idType) {
            return false;
        }
        return BytesUtil.compare(this.bytes, this.length,
                                 other.bytes, other.length) == 0;
    }

    @Override
    public int hashCode() {
        return BytesUtil.hashBytes(this.bytes, this.length);
    }

    @Override
    public String toString() {
        return this.asObject().toString();
    }
}
