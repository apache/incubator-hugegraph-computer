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

package com.baidu.hugegraph.computer.algorithm.path.links;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.NotSupportedException;

import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class LinksValue implements Value<LinksValue> {

    private final List<LinksValueItem> values;

    public LinksValue() {
        this.values = new ArrayList<>();
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<LinksValue> value) {
        throw new NotSupportedException();
    }

    @Override
    public Value<LinksValue> copy() {
        throw new NotSupportedException();
    }

    @Override
    public Object object() {
        throw new NotSupportedException();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.values.clear();
        int count = in.readInt();
        LinksValueItem value;
        for (int i = 0; i < count; i++) {
            value = new LinksValueItem();
            value.read(in);
            this.values.add(value);
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.values.size());
        for (LinksValueItem value : this.values) {
            value.write(out);
        }
    }

    @Override
    public int compareTo(LinksValue o) {
        throw new NotSupportedException();
    }

    public void addValue(IdList vertexes, IdList edges) {
        this.values.add(new LinksValue.LinksValueItem(vertexes, edges));
    }

    @Override
    public String toString() {
        return this.values.toString();
    }

    public List<LinksValueItem> values() {
        return Collections.unmodifiableList(this.values);
    }

    public int size() {
        return this.values.size();
    }

    public static class LinksValueItem implements Value<LinksValueItem> {

        private final IdList vertexes;
        private final IdList edges;

        public LinksValueItem() {
            this.vertexes = new IdList();
            this.edges = new IdList();
        }

        public LinksValueItem(IdList vertexes, IdList edges) {
            this.vertexes = vertexes;
            this.edges = edges;
        }

        @Override
        public ValueType valueType() {
            return ValueType.UNKNOWN;
        }

        @Override
        public void assign(Value<LinksValueItem> value) {
            throw new NotSupportedException();
        }

        @Override
        public Value<LinksValueItem> copy() {
            throw new NotSupportedException();
        }

        @Override
        public Object object() {
            throw new NotSupportedException();
        }

        @Override
        public void read(RandomAccessInput in) throws IOException {
            this.vertexes.read(in);
            this.edges.read(in);
        }

        @Override
        public void write(RandomAccessOutput out) throws IOException {
            this.vertexes.write(out);
            this.edges.write(out);
        }

        @Override
        public int compareTo(LinksValueItem other) {
            throw new NotSupportedException();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder("{");
            for (int i = 0; i < this.vertexes.size(); i++) {
                builder.append(this.vertexes.get(i).toString());
                if (i < this.vertexes.size() - 1) {
                    builder.append(", ");
                    builder.append(this.edges.get(i).toString());
                    builder.append(", ");
                }
            }
            builder.append("}");
            return builder.toString();
        }
    }
}
