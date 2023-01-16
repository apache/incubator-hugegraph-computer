/*
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

package org.apache.hugegraph.computer.algorithm.community.cc;

import java.io.IOException;

import org.apache.hugegraph.computer.core.graph.value.IdSet;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;

/**
 * TODO: We could reuse triangle's result to simplify it (and avoid logical differences)
 */
public class ClusteringCoefficientValue implements Value.CustomizeValue<Integer> {

    private IdSet idSet;
    private IntValue count;
    private final IntValue degree;

    public ClusteringCoefficientValue() {
        this.idSet = new IdSet();
        this.count = new IntValue();
        this.degree = new IntValue();
    }

    public IdSet idSet() {
        return this.idSet;
    }

    public int count() {
        return this.count.intValue();
    }

    public void count(Integer count) {
        this.count.value(count);
    }

    public int degree() {
        return this.degree.value();
    }

    public void degree(Integer degree) {
        this.degree.value(degree);
    }

    @Override
    public ClusteringCoefficientValue copy() {
        ClusteringCoefficientValue ccValue = new ClusteringCoefficientValue();
        ccValue.idSet = this.idSet.copy();
        ccValue.count = this.count.copy();
        return ccValue;
    }

    @Override
    public Integer value() {
        return this.count.value();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.idSet.read(in);
        this.count.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.idSet.write(out);
        this.count.write(out);
    }

    @Override
    public String toString() {
        return String.valueOf(count);
    }
}
