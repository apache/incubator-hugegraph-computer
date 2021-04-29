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
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.collections.ListUtils;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.SerialEnum;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.util.E;

public class ListValue<T extends Value<?>> implements Value<ListValue<T>> {

    // TODO: try to reduce call ComputerContext.instance() directly.
    private static final GraphFactory GRAPH_FACTORY =
                                      ComputerContext.instance().graphFactory();
    private static final ValueFactory VALUE_FACTORY =
                                      ComputerContext.instance().valueFactory();

    private ValueType elemType;
    private List<T> values;

    public ListValue() {
        this(ValueType.UNKNOWN);
    }

    public ListValue(ValueType elemType) {
        this(elemType, GRAPH_FACTORY.createList());
    }

    public ListValue(ValueType elemType, List<T> values) {
        this.elemType = elemType;
        this.values = values;
    }

    public void add(T value) {
        E.checkArgument(value != null, "The value to be added can't be null");
        if (this.elemType != ValueType.UNKNOWN) {
            E.checkArgument(this.elemType == value.type(),
                            "The value to be added can't be null and type " +
                            "should be %s, actual is %s",
                            this.elemType, value);
        } else {
            this.elemType = value.type();
        }
        this.values.add(value);
    }

    public T get(int index) {
        return this.values.get(index);
    }

    public T getLast() {
        int index = this.values.size() - 1;
        if (index < 0) {
            throw new NoSuchElementException("The list value is empty");
        }
        return this.values.get(index);
    }

    public ListValue<T> copy() {
        List<T> values = GRAPH_FACTORY.createList();
        values.addAll(this.values);
        return new ListValue<>(this.elemType, values);
    }

    public boolean contains(T obj) {
        return this.values.contains(obj);
    }

    public List<T> values() {
        return Collections.unmodifiableList(this.values);
    }

    public int size() {
        return this.values.size();
    }

    @Override
    public ValueType type() {
        return ValueType.LIST_VALUE;
    }

    public ValueType elemType() {
        return this.elemType;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.read(in, true);
    }

    protected void read(RandomAccessInput in, boolean readElemType)
                        throws IOException {
        int size = in.readInt();
        if (readElemType) {
            this.elemType = SerialEnum.fromCode(ValueType.class,
                                                in.readByte());
        }
        this.values = GRAPH_FACTORY.createList(size);
        for (int i = 0; i < size; i++) {
            @SuppressWarnings("unchecked")
            T value = (T) VALUE_FACTORY.createValue(this.elemType);
            value.read(in);
            this.values.add(value);
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.write(out, true);
    }

    protected void write(RandomAccessOutput out, boolean writeElemType)
                         throws IOException {
        out.writeInt(this.values.size());
        if (writeElemType) {
            out.writeByte(this.elemType.code());
        }
        for (T value : this.values) {
            value.write(out);
        }
    }

    @Override
    public int compareTo(ListValue<T> obj) {
        E.checkArgumentNotNull(obj, "The compare argument can't be null");
        int cmp = this.size() - obj.size();
        if (cmp != 0) {
            return cmp;
        }
        for (int i = 0; i < this.size(); i++) {
            @SuppressWarnings("unchecked")
            Value<Object> self = (Value<Object>) this.values.get(i);
            cmp = self.compareTo(obj.values.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ListValue)) {
            return false;
        }
        @SuppressWarnings("unchecked")
        ListValue<T> other = (ListValue<T>) obj;
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
