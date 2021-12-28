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

package com.baidu.hugegraph.computer.algorithm.community.kcore;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class KCoreValue implements Value<KCoreValue> {

    private int core;
    private Set<Id> deletedNeighbors;

    public KCoreValue() {
        this.core = 0;
        this.deletedNeighbors = new HashSet<>();
    }

    public void core(int core) {
        assert core >= 0;
        this.core = core;
    }

    public int core() {
        return this.core;
    }

    public int decreaseCore(int decrease) {
        assert decrease <= this.core;
        this.core -= decrease;
        return this.core;
    }

    public boolean active() {
        return this.core > 0;
    }

    public boolean isNeighborDeleted(Id neighborId) {
        return this.deletedNeighbors.contains(neighborId);
    }

    public void addDeletedNeighbor(Id neighborId) {
        this.deletedNeighbors.add(neighborId);
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<KCoreValue> other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value<KCoreValue> copy() {
        KCoreValue kcoreValue = new KCoreValue();
        kcoreValue.core = this.core;
        return kcoreValue;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.core = in.readInt();
        int size = in.readInt();
        this.deletedNeighbors = new HashSet<>((int)((float) size / 0.75 + 1));
        for (int i = 0; i < size; i++) {
            Id id = new BytesId();
            id.read(in);
            deletedNeighbors.add(id);
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.core);
        out.writeInt(this.deletedNeighbors.size());
        for (Id id : this.deletedNeighbors) {
            id.write(out);
        }
    }

    @Override
    public int compareTo(KCoreValue other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                   .append("core", this.core)
                   .toString();
    }

    @Override
    public String string() {
        return String.valueOf(this.value());
    }

    @Override
    public Object value() {
        return this.core;
    }
}
