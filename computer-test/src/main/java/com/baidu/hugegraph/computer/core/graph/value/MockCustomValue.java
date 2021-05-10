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
import java.util.Date;

import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class MockCustomValue implements Value<MockCustomValue> {

    private Date value;

    public MockCustomValue() {
        this.value = new Date();
    }

    @Override
    public ValueType type() {
        return ValueType.CUSTOM_VALUE;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.value.setTime(in.readLong());
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeLong(this.value.getTime());
    }

    @Override
    public int compareTo(MockCustomValue o) {
        return this.value.compareTo(o.value);
    }
}
