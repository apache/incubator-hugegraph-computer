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

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class StringValue implements Value<StringValue> {

    private String value;

    public StringValue() {
        this.value = Constants.EMPTY_STR;
    }

    public StringValue(String value) {
        this.value = value;
    }

    @Override
    public ValueType valueType() {
        return ValueType.STRING;
    }

    @Override
    public void assign(Value<StringValue> other) {
        this.checkAssign(other);
        this.value = ((StringValue) other).value;
    }

    @Override
    public StringValue copy() {
        return new StringValue(this.value);
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.value = in.readUTF();
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeUTF(this.value);
    }

    @Override
    public int compareTo(StringValue other) {
        return this.value.compareTo(other.value);
    }

    public String value() {
        return this.value;
    }

    public void value(String value) {
        this.value = value;
    }

    @Override
    public Object object() {
        return this.value;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StringValue)) {
            return false;
        }
        return ((StringValue) obj).value.equals(this.value);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public String toString() {
        return this.value;
    }
}
