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

package com.baidu.hugegraph.computer.algorithm.path.subgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class SubGraphMatchMessage implements Value<SubGraphMatchMessage> {

    private List<Pair<Integer, Id>> matchPath;

    public SubGraphMatchMessage() {
        this.matchPath = new LinkedList<>();
    }

    public void merge(Pair<Integer, Id> pair) {
        this.matchPath.add(pair);
    }

    public List<Pair<Integer, Id>> matchPath() {
        return this.matchPath;
    }

    public Pair<Integer, Id> lastNode() {
        return this.matchPath.get(this.matchPath.size() - 1);
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<SubGraphMatchMessage> value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value<SubGraphMatchMessage> copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object value() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.matchPath = new ArrayList<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            int nodeId = in.readInt();
            BytesId id = new BytesId();
            id.read(in);
            matchPath.add(new MutablePair<>(nodeId, id));
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.matchPath.size());
        for (Pair<Integer, Id> pair : this.matchPath) {
            out.writeInt(pair.getLeft());
            pair.getRight().write(out);
        }
    }

    @Override
    public int compareTo(SubGraphMatchMessage o) {
        throw new UnsupportedOperationException();
    }
}
