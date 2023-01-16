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

public class IntValue extends Number implements Tvalue<Integer> {

    private static final long serialVersionUID = -2014388310992178979L;

    private int value;

    public IntValue() {
        this(0);
    }

    public IntValue(int value) {
        this.value = value;
    }

    @Override
    public int intValue() {
        return this.value;
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
    public Integer value() {
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
    public ValueType valueType() {
        return ValueType.INT;
    }

    @Override
    public void assign(Value other) {
        this.checkAssign(other);
        this.value = ((IntValue) other).value;
    }

    @Override
    public IntValue copy() {
        return new IntValue(this.value);
    }

    @Override
    public boolean isNumber() {
        return true;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.value = in.readInt();
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.value);
    }

    @Override
    public int compareTo(Value obj) {
        E.checkArgumentNotNull(obj, "The compare argument can't be null");
        int typeDiff = this.valueType().compareTo(obj.valueType());
        if (typeDiff != 0) {
            return typeDiff;
        }
        return Integer.compare(this.value, ((IntValue) obj).value);
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
