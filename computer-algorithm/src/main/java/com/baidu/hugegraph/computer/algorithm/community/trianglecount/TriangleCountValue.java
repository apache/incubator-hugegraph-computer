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

package com.baidu.hugegraph.computer.algorithm.community.trianglecount;

import java.io.IOException;

import javax.ws.rs.NotSupportedException;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class TriangleCountValue implements Value<TriangleCountValue> {

    private IdList idList;
    private LongValue count;

    public TriangleCountValue() {
        this.idList = new IdList();
        this.count = new LongValue();
    }

    public IdList idList() {
        return this.idList;
    }

    public long count() {
        return this.count.value();
    }

    public void count(Long count) {
        this.count.value(count);
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<TriangleCountValue> other) {
        throw new NotSupportedException();
    }

    @Override
    public Value<TriangleCountValue> copy() {
        TriangleCountValue triangleCountValue = new TriangleCountValue();
        triangleCountValue.idList = this.idList.copy();
        triangleCountValue.count = this.count.copy();
        return triangleCountValue;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.idList.read(in);
        this.count.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.idList.write(out);
        this.count.write(out);
    }

    @Override
    public int compareTo(TriangleCountValue other) {
        throw new NotSupportedException();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                   .append("idList", this.idList)
                   .append("count", this.count)
                   .toString();
    }
}
