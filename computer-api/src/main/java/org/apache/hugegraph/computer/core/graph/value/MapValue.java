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

package org.apache.hugegraph.computer.core.graph.value;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.SerialEnum;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.util.E;

public class MapValue<T extends Value> implements Value {

    private final GraphFactory graphFactory;

    private ValueType elemType;
    private Map<Id, T> map;

    public MapValue() {
        this(ValueType.UNKNOWN);
    }

    public MapValue(ValueType elemType) {
        this(elemType, new HashMap<>());
    }

    public MapValue(ValueType elemType, Map<Id, T> map) {
        this.graphFactory = ComputerContext.instance().graphFactory();
        this.elemType = elemType;
        this.map = map;
    }

    public void put(Id id, T value) {
        E.checkArgument(id != null, "Can't add null key to %s",
                        this.valueType().string());
        E.checkArgument(value != null, "Can't add null value to %s",
                        this.valueType().string());
        if (this.elemType != ValueType.UNKNOWN) {
            E.checkArgument(this.elemType == value.valueType(),
                            "Invalid value '%s' with type %s, " +
                            "expect element with type %s",
                            value, value.valueType().string(),
                            this.elemType.string());
        } else {
            this.elemType = value.valueType();
        }
        this.map.put(id, value);
    }

    public T get(Id id) {
        return this.map.get(id);
    }

    public Set<Map.Entry<Id, T>> entrySet() {
        return this.map.entrySet();
    }

    @Override
    public ValueType valueType() {
        return ValueType.MAP_VALUE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void assign(Value other) {
        this.checkAssign(other);
        ValueType elemType = ((MapValue<T>) other).elemType();
        E.checkArgument(elemType == this.elemType(),
                        "Can't assign %s<%s> to %s<%s>",
                        other.valueType().string(), elemType.string(),
                        this.valueType().string(), this.elemType().string());
        this.map = ((MapValue<T>) other).map;
    }

    public ValueType elemType() {
        return this.elemType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Value copy() {
        Map<Id, T> map = new HashMap<>();
        for (Map.Entry<Id, T> entry : this.map.entrySet()) {
            map.put(entry.getKey(), (T) entry.getValue().copy());
        }
        return new MapValue<>(this.elemType, map);
    }

    @Override
    public Object value() {
        return this.map;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.read(in, true);
    }

    protected void read(RandomAccessInput in, boolean readElemType)
                        throws IOException {
        int size = in.readInt();
        if (readElemType) {
            this.elemType = SerialEnum.fromCode(ValueType.class, in.readByte());
        }
        this.map = this.graphFactory.createMap();

        for (int i = 0; i < size; i++) {
            Id id = this.graphFactory.createId();
            id.read(in);
            @SuppressWarnings("unchecked")
            T value = (T) this.graphFactory.createValue(this.elemType);
            value.read(in);
            this.map.put(id, value);
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.write(out, true);
    }

    protected void write(RandomAccessOutput out, boolean writeElemType)
                         throws IOException {
        out.writeInt(this.map.size());
        if (writeElemType) {
            out.writeByte(this.elemType.code());
        }
        for (Map.Entry<Id, T> entry : this.map.entrySet()) {
            Id id = entry.getKey();
            T value = entry.getValue();
            id.write(out);
            value.write(out);
        }
    }

    @Override
    public int compareTo(Value obj) {
        throw new UnsupportedOperationException("MapValue.compareTo()");
    }

    @Override
    public String string() {
        return "MapValue{elemType=" + this.elemType + ", map=" + this.map + "}";
    }
}
