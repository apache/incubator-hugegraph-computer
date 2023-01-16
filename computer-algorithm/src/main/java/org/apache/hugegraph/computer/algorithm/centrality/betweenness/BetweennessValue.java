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

package org.apache.hugegraph.computer.algorithm.centrality.betweenness;

import java.io.IOException;

import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.IdSet;
import org.apache.hugegraph.computer.core.graph.value.Value.CustomizeValue;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;

public class BetweennessValue implements CustomizeValue<Double> {

    private final DoubleValue betweenness;
    private final IdSet arrivedVertices;

    public BetweennessValue() {
        this(0.0D);
    }

    public BetweennessValue(double betweenness) {
        this.betweenness = new DoubleValue(betweenness);
        this.arrivedVertices = new IdSet();
    }

    public DoubleValue betweenness() {
        return this.betweenness;
    }

    public IdSet arrivedVertices() {
        return this.arrivedVertices;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.betweenness.read(in);
        this.arrivedVertices.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.betweenness.write(out);
        this.arrivedVertices.write(out);
    }

    @Override
    public Double value() {
        return this.betweenness.value();
    }

    @Override
    public String string() {
        return this.value().toString();
    }
}
