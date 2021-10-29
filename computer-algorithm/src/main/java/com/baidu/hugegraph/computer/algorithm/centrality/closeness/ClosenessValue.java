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

package com.baidu.hugegraph.computer.algorithm.centrality.closeness;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class ClosenessValue implements Value<ClosenessValue> {

    private final GraphFactory graphFactory;

    private Map<Id, DoubleValue> map;

    public ClosenessValue() {
        this.graphFactory = ComputerContext.instance().graphFactory();
        this.map = this.graphFactory.createMap();
    }

    public DoubleValue get(Id id) {
        return this.map.get(id);
    }

    public void put(Id id, DoubleValue value) {
        this.map.put(id, value);
    }

    public Set<Map.Entry<Id, DoubleValue>> entrySet() {
        return this.map.entrySet();
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<ClosenessValue> value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value<ClosenessValue> copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object value() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        int size = in.readInt();
        this.map = this.graphFactory.createMap();
        for (int i = 0; i < size; i++) {
            Id id = this.graphFactory.createId();
            id.read(in);
            Value<?> value = this.graphFactory.createValue(ValueType.DOUBLE);
            value.read(in);
            this.map.put(id, (DoubleValue) value);
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.map.size());
        for (Map.Entry<Id, DoubleValue> entry : this.map.entrySet()) {
            Id id = entry.getKey();
            DoubleValue value = entry.getValue();
            id.write(out);
            value.write(out);
        }
    }

    @Override
    public int compareTo(ClosenessValue o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String string() {
        return String.valueOf(this.map);
    }
}
