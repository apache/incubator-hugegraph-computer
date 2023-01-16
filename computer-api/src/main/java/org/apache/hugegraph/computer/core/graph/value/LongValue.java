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

package org.apache.hugegraph.computer.core.graph.value;

import java.io.IOException;

import org.apache.hugegraph.computer.core.graph.value.Value.Tvalue;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.util.E;

public class LongValue extends Number implements Tvalue<Long> {

    private static final long serialVersionUID = 8332327679205404212L;

    private long value;

    public LongValue() {
        this(0L);
    }

    public LongValue(long value) {
        this.value = value;
    }

    @Override
    public int intValue() {
        return (int) this.value;
    }

    @Override
    public long longValue() {
        return this.value;
    }

    @Override
    public float floatValue() {
        return this.value;
    }

    @Override
    public double doubleValue() {
        return this.value;
    }

    @Override
    public Long value() {
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
    public ValueType valueType() {
        return ValueType.LONG;
    }

    @Override
    public void assign(Value other) {
        this.checkAssign(other);
        this.value = ((LongValue) other).value;
    }

    @Override
    public LongValue copy() {
        return new LongValue(this.value);
    }

    @Override
    public boolean isNumber() {
        return true;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.value = in.readLong();
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeLong(this.value);
    }

    @Override
    public int compareTo(Value obj) {
        E.checkArgumentNotNull(obj, "The compare argument can't be null");
        int typeDiff = this.valueType().compareTo(obj.valueType());
        if (typeDiff != 0) {
            return typeDiff;
        }
        return Long.compare(this.value, ((LongValue) obj).value);
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
