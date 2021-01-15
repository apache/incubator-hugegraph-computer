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

import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.util.E;

public abstract class ListValue<T extends Value> implements Value {

    private ValueType elemType;
    private List<T> values;

    public ListValue(ValueType elemType) {
        this.elemType = elemType;
        this.values = new LinkedList<>();
    }

    public void add(T value) {
        E.checkArgument(value != null && this.elemType == value.type(),
                        "The value to be added can't be null and type " +
                        "should be %s, actual is %s", this.elemType, value);
        this.values.add(value);
    }

    public List<T> values() {
        return Collections.unmodifiableList(this.values);
    }

    public ValueType elemType() {
        return this.elemType;
    }

    @Override
    public void read(StreamGraphInput in) throws IOException {
        int size = in.readVInt();
        this.values = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            @SuppressWarnings("unchecked")
            T value = (T) ValueFactory.createValue(this.elemType);
            value.read(in);
            this.values.add(value);
        }
    }

    @Override
    public void write(StreamGraphOutput out) throws IOException {
        out.writeVInt(this.values.size());
        for (T value : this.values) {
            value.write(out);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ListValue)) {
            return false;
        }
        ListValue other = (ListValue) obj;
        if (this.elemType != other.elemType) {
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
        return String.format("ListValue{elemType=%s" + ", size=%s}",
                             this.elemType, this.values.size());
    }
}
