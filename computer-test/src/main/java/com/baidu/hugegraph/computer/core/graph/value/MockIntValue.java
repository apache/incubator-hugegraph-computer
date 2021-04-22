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

import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;

public class MockIntValue implements Value<MockIntValue> {

    private int value;

    @Override
    public ValueType type() {
        return ValueType.USER_DEFINED;
    }

    @Override
    public void read(GraphInput in) throws IOException {
        this.value = in.readInt();
    }

    @Override
    public void write(GraphOutput out) throws IOException {
        out.writeInt(this.value);
    }

    @Override
    public int compareTo(MockIntValue o) {
        return Integer.compare(this.value, o.value);
    }
}
