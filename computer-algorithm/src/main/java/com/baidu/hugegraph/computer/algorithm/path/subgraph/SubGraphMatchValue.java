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
import java.util.List;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class SubGraphMatchValue implements Value<SubGraphMatchValue> {

    private final IdListList res;
    private final List<List<Pair<Integer, Id>>> mp;

    public SubGraphMatchValue() {
        this.res = new IdListList();
        this.mp = new ArrayList<>();
    }

    public List<List<Pair<Integer, Id>>> mp() {
        return this.mp;
    }

    public IdListList res() {
        return this.res;
    }

    public void addRes(IdList ids) {
        ids.sort();
        for (int i = 0; i < res.size(); i++) {
            IdList item = this.res.get(i);
            if (ids.equals(item)) {
                return;
            }
        }
        this.res.add(ids);
    }

    public void addMp(List<Pair<Integer, Id>> path) {
        this.mp.add(path);
    }

    public void clearMp() {
        this.mp.clear();
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<SubGraphMatchValue> value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value<SubGraphMatchValue> copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object value() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.res.read(in);

        this.mp.clear();
        int mpSize = in.readInt();
        for (int i = 0; i < mpSize; i++) {
            List<Pair<Integer, Id>> pairs = new ArrayList<>();
            this.mp.add(pairs);
            int pairSize = in.readInt();
            for (int j = 0; j < pairSize; j++) {
                int nodeId = in.readInt();
                Id id = new BytesId();
                id.read(in);
                pairs.add(new MutablePair<>(nodeId, id));
            }
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.res.write(out);

        out.writeInt(this.mp.size());
        for (List<Pair<Integer, Id>> pairs : this.mp) {
            out.writeInt(pairs.size());
            for (Pair<Integer, Id> pair : pairs) {
                out.writeInt(pair.getLeft());
                pair.getRight().write(out);
            }
        }
    }

    @Override
    public String string() {
        return this.res.toString();
    }

    @Override
    public int compareTo(SubGraphMatchValue o) {
        throw new UnsupportedOperationException();
    }
}
