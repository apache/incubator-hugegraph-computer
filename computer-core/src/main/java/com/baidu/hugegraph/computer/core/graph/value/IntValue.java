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
import com.baidu.hugegraph.util.E;

public class IntValue implements Value<IntValue> {

    private int value;

    public IntValue() {
        this.value = 0;
    }

    public IntValue(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }

    /*
     * This method is reserved for performance, otherwise it will create a new
     * IntValue object when change it's value.
     */
    public void value(int value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.INT;
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
    public int compareTo(IntValue obj) {
        E.checkArgumentNotNull(obj, "The compare argument can't be null");
        return Integer.compare(this.value, obj.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IntValue)) {
            return false;
        }
        return ((IntValue) obj).value == this.value;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(this.value);
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }
}
