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

package com.baidu.hugegraph.computer.core.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongValue implements Value {

    private long value;

    public LongValue() {
        this.value = 0;
    }

    public LongValue(long value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.LONG;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.value);
    }

    @Override
    public void read(DataInput in) throws IOException {
        this.value = in.readLong();
    }

    public long value() {
        return this.value;
    }

    /*
     * This method is reserved for performance, otherwise it will create a new
     * LongValue object when change it's value.
     */
    public void value(long value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LongValue)) {
            return false;
        }
        return ((LongValue) obj).value == this.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.value);
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

}
