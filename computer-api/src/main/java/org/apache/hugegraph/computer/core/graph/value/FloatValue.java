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

public class FloatValue extends Number implements Tvalue<Float> {

    private static final long serialVersionUID = 6098857579782490901L;

    private float value;

    public FloatValue() {
        this(0.0F);
    }

    public FloatValue(float value) {
        this.value = value;
    }

    @Override
    public int intValue() {
        return (int) this.value;
    }

    @Override
    public long longValue() {
        return (long) this.value;
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
    public Float value() {
        return this.value;
    }

    /*
     * This method is reserved for performance, otherwise it will create a new
     * DoubleValue object when change it's value.
     */
    public void value(float value) {
        this.value = value;
    }

    @Override
    public ValueType valueType() {
        return ValueType.FLOAT;
    }

    @Override
    public void assign(Value other) {
        this.checkAssign(other);
        this.value = ((FloatValue) other).value;
    }

    @Override
    public FloatValue copy() {
        return new FloatValue(this.value);
    }

    @Override
    public boolean isNumber() {
        return true;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.value = in.readFloat();
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeFloat(this.value);
    }

    @Override
    public int compareTo(Value obj) {
        E.checkArgumentNotNull(obj, "The compare argument can't be null");
        int typeDiff = this.valueType().compareTo(obj.valueType());
        if (typeDiff != 0) {
            return typeDiff;
        }
        return Float.compare(this.value, ((FloatValue) obj).value);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FloatValue)) {
            return false;
        }
        return ((FloatValue) obj).value == this.value;
    }

    @Override
    public int hashCode() {
        return Float.hashCode(this.value);
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }
}
