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

package org.apache.hugegraph.computer.core.graph.properties;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.hugegraph.computer.core.common.SerialEnum;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;

public class DefaultProperties implements Properties {

    private final Map<String, Value> keyValues;
    private final GraphFactory graphFactory;

    public DefaultProperties(GraphFactory graphFactory) {
        this(graphFactory.createMap(), graphFactory);
    }

    public DefaultProperties(Map<String, Value> keyValues,
                             GraphFactory graphFactory) {
        this.keyValues = keyValues;
        this.graphFactory = graphFactory;
    }

    @Override
    public Map<String, Value> get() {
        return Collections.unmodifiableMap(this.keyValues);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Value> T get(String key) {
        return (T) this.keyValues.get(key);
    }

    @Override
    public void put(String key, Value value) {
        this.keyValues.put(key, value);
    }

    @Override
    public void putIfAbsent(String key, Value value) {
        this.keyValues.putIfAbsent(key, value);
    }

    @Override
    public void putAll(Map<String, Value> kvs) {
        this.keyValues.putAll(kvs);
    }

    public int size() {
        return this.keyValues.size();
    }

    @Override
    public void clear() {
        this.keyValues.clear();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.keyValues.clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            ValueType valueType = SerialEnum.fromCode(ValueType.class,
                                                      in.readByte());
            Value value = this.graphFactory.createValue(valueType);
            value.read(in);
            this.keyValues.put(key, value);
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.keyValues.size());
        for (Map.Entry<String, Value> entry : this.keyValues.entrySet()) {
            out.writeUTF(entry.getKey());
            Value value = entry.getValue();
            out.writeByte(value.valueType().code());
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
