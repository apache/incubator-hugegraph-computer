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

package com.baidu.hugegraph.computer.core.graph.properties;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import com.baidu.hugegraph.computer.core.common.SerialEnum;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactory;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class DefaultProperties implements Properties {

    private final Map<String, Value<?>> keyValues;
    private final ValueFactory valueFactory;

    public DefaultProperties(GraphFactory graphFactory,
                             ValueFactory valueFactory) {
        this(graphFactory.createMap(), valueFactory);
    }

    public DefaultProperties(Map<String, Value<?>> keyValues,
                             ValueFactory valueFactory) {
        this.keyValues = keyValues;
        this.valueFactory = valueFactory;
    }

    @Override
    public Map<String, Value<?>> get() {
        return this.keyValues;
    }

    @Override
    public void put(String key, Value<?> value) {
        this.keyValues.put(key, value);
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.keyValues.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            ValueType valueType = SerialEnum.fromCode(ValueType.class,
                                                      in.readByte());
            Value<?> value = this.valueFactory.createValue(valueType);
            value.read(in);
            this.keyValues.put(key, value);
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.keyValues.size());
        for (Map.Entry<String, Value<?>> entry : this.keyValues.entrySet()) {
            out.writeUTF(entry.getKey());
            Value<?> value = entry.getValue();
            out.writeByte(value.type().code());
            value.write(out);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof DefaultProperties)) {
            return false;
        }
        DefaultProperties other = (DefaultProperties) obj;
        return this.keyValues.equals(other.keyValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.keyValues);
    }

    @Override
    public String toString() {
        return String.format("DefaultProperties{keyValues=%s}",
                             this.keyValues);
    }
}
