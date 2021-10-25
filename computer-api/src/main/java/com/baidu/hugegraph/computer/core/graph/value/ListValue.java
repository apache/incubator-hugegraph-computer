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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.SerialEnum;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.util.E;

public class ListValue<T extends Value<?>> implements Value<ListValue<T>> {

    private final GraphFactory graphFactory;
    private ValueType elemType;
    private List<T> values;

    public ListValue() {
        this(ValueType.UNKNOWN);
    }

    public ListValue(ValueType elemType) {
        this(elemType, new ArrayList<>());
    }

    public ListValue(ValueType elemType, List<T> values) {
        this.elemType = elemType;
        this.values = values;
        // TODO: try to reduce call ComputerContext.instance() directly.
        this.graphFactory = ComputerContext.instance().graphFactory();
    }

    public void checkAndSetType(T value) {
        E.checkArgument(value != null,
                        "Can't add null to %s", this.valueType().string());
        if (this.elemType != ValueType.UNKNOWN) {
            E.checkArgument(this.elemType == value.valueType(),
                            "Invalid value '%s' with type %s, " +
                            "expect element with type %s",
                            value, value.valueType().string(),
                            this.elemType.string());
        } else {
            this.elemType = value.valueType();
        }
    }

    public void add(T value) {
        this.checkAndSetType(value);
        this.values.add(value);
    }

    public void addAll(Collection<T> values) {
        if (CollectionUtils.isEmpty(values)) {
            return;
        }

        Iterator<T> iterator = values.iterator();
        T firstValue = iterator.next();
        this.checkAndSetType(firstValue);
        this.values.addAll(values);
    }

    public T get(int index) {
        return this.values.get(index);
    }

    public T getLast() {
        int index = this.values.size() - 1;
        if (index < 0) {
            throw new NoSuchElementException("The list is empty");
        }
        return this.values.get(index);
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
    public ValueType valueType() {
        return ValueType.LIST_VALUE;
    }

    public ValueType elemType() {
        return this.elemType;
    }

    @Override
    public void assign(Value<ListValue<T>> other) {
        this.checkAssign(other);
        ValueType elemType = ((ListValue<T>) other).elemType();
        E.checkArgument(elemType == this.elemType(),
                        "Can't assign %s<%s> to %s<%s>",
                        other.valueType().string(), elemType.string(),
                        this.valueType().string(), this.elemType().string());
        this.values = ((ListValue<T>) other).values();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ListValue<T> copy() {
        List<T> values = this.graphFactory.createList();
        for (T value : this.values) {
            values.add((T) value.copy());
        }
        return new ListValue<>(this.elemType, values);
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
        if (size > this.values.size() || size < this.values.size() / 2) {
            this.values = this.graphFactory.createList(size);
        } else {
            this.values.clear();
        }

        for (int i = 0; i < size; i++) {
            @SuppressWarnings("unchecked")
            T value = (T) this.graphFactory.createValue(this.elemType);
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
        return this.values.toString();
    }

    @Override
    public List<T> value() {
        return this.values;
    }
}
