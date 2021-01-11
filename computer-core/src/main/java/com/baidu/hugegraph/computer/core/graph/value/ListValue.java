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

package com.baidu.hugegraph.computer.core.graph.value;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.collections.ListUtils;

import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.util.E;

public class ListValue implements Value {

    private ValueType valueType;
    private List<Value> values;

    public ListValue(ValueType valueType) {
        this.valueType = valueType;
        this.values = new LinkedList<>();
    }

    public void add(Value value) {
        E.checkArgument(value != null && this.valueType == value.type(),
                        "The value to be added can't be null and type " +
                        "should be %s, actual is %s", this.valueType, value);
        this.values.add(value);
    }

    public List<Value> values() {
        return Collections.unmodifiableList(this.values);
    }

    @Override
    public Cardinality cardinality() {
        return Cardinality.LIST;
    }

    @Override
    public ValueType type() {
        return this.valueType;
    }

    @Override
    public void read(GraphInput in) throws IOException {
        int size = in.readVInt();
        this.values = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            Value value = ValueFactory.createValue(Cardinality.SINGLE,
                                                   this.valueType);
            value.read(in);
            this.values.add(value);
        }
    }

    @Override
    public void write(GraphOutput out) throws IOException {
        out.writeVInt(this.values.size());
        for (Value value : this.values) {
            value.write(out);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ListValue)) {
            return false;
        }
        ListValue other = (ListValue) obj;
        if (this.valueType != other.valueType) {
            return false;
        }
        return ListUtils.isEqualList(this.values, other.values);
    }

    @Override
    public int hashCode() {
        return ListUtils.hashCodeForList(this.values);
    }

    @Override
    public String toString() {
        return "ListValue{valueType=" + this.valueType +
                ", size=" + this.values.size() + "}";
    }
}
