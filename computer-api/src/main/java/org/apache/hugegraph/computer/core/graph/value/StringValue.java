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

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.graph.value.Value.Tvalue;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.util.E;

public class StringValue implements Tvalue<String> {

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
    public void assign(Value other) {
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
    public int compareTo(Value obj) {
        E.checkArgumentNotNull(obj, "The compare argument can't be null");
        int typeDiff = this.valueType().compareTo(obj.valueType());
        if (typeDiff != 0) {
            return typeDiff;
        }
        return this.value.compareTo(((StringValue) obj).value);
    }

    @Override
    public String value() {
        return this.value;
    }

    public void value(String value) {
        this.value = value;
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
