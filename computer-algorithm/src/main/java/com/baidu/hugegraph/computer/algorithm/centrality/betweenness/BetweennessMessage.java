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

package com.baidu.hugegraph.computer.algorithm.centrality.betweenness;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.util.E;

public class BetweennessMessage implements Value<BetweennessMessage> {

    private final IdList sequence;
    private final DoubleValue vote;

    public BetweennessMessage() {
        this.sequence = new IdList();
        this.vote = new DoubleValue(0.0D);
    }

    public BetweennessMessage(IdList sequence) {
        this.sequence = sequence;
        this.vote = new DoubleValue();
    }

    public BetweennessMessage(DoubleValue betweenness) {
        this.sequence = new IdList();
        this.vote = betweenness;
    }

    public IdList sequence() {
        return this.sequence;
    }

    public DoubleValue vote() {
        return this.vote;
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<BetweennessMessage> value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value<BetweennessMessage> copy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object value() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.sequence.read(in);
        this.vote.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.sequence.write(out);
        this.vote.write(out);
    }

    @Override
    public int compareTo(BetweennessMessage other) {
        E.checkArgument(this.sequence.size() != 0, "Sequence can't be empty");
        E.checkArgument(other.sequence.size() != 0, "Sequence can't be empty");
        Id selfSourceId = this.sequence.get(0);
        Id otherSourceId = other.sequence.get(0);
        return selfSourceId.compareTo(otherSourceId);
    }
}
