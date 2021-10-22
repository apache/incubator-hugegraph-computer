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

package com.baidu.hugegraph.computer.algorithm.community.cc;

import java.io.IOException;

import javax.ws.rs.NotSupportedException;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

/**
 * We should reuse triangle
 */
public class ClusteringCoefficientValue implements
                                        Value<ClusteringCoefficientValue> {
    private IdList idList;
    private LongValue count;
    private IntValue degree;

    public ClusteringCoefficientValue() {
        this.idList = new IdList();
        this.count = new LongValue();
        this.degree = new IntValue();
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

    public int degree() {
        return this.degree.value();
    }

    public void setDegree(Integer degree) {
        this.degree.value(degree);
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<ClusteringCoefficientValue> other) {
        throw new NotSupportedException();
    }

    @Override
    public Value<ClusteringCoefficientValue> copy() {
        ClusteringCoefficientValue ccValue = new ClusteringCoefficientValue();
        ccValue.idList = this.idList.copy();
        ccValue.count = this.count.copy();
        return ccValue;
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
    public int compareTo(ClusteringCoefficientValue other) {
        throw new NotSupportedException();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                   .append("degree", this.degree)
                   .toString();
    }

    @Override
    public Object object() {
        throw new NotSupportedException();
    }
}
