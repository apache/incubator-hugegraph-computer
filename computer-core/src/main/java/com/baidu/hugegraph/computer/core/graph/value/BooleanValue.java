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

public class BooleanValue implements Value {

    private boolean value;

    public BooleanValue() {
        this.value = false;
    }

    public BooleanValue(boolean value) {
        this.value = value;
    }

    public boolean value() {
        return this.value;
    }

    /*
     * This method is reserved for performance, otherwise it will create a new
     * IntValue object when change it's value.
     */
    public void value(boolean value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.BOOLEAN;
    }

    @Override
    public void read(GraphInput in) throws IOException {
        this.value = in.readBoolean();
    }

    @Override
    public void write(GraphOutput out) throws IOException {
        out.writeBoolean(this.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BooleanValue)) {
            return false;
        }
        return ((BooleanValue) obj).value == this.value;
    }

    @Override
    public int hashCode() {
        return Boolean.hashCode(this.value);
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }
}
