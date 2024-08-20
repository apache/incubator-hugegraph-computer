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

package org.apache.hugegraph.computer.algorithm.community.kcore;

import java.io.IOException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.id.IdFactory;
import org.apache.hugegraph.computer.core.graph.value.Value.CustomizeValue;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.util.E;

public class KcoreValue implements CustomizeValue<Object> {

    private int degree;
    private Id core;

    public KcoreValue() {
        this.degree = 0;
        this.core = IdFactory.createId();
    }

    public void degree(int degree) {
        assert degree >= 0;
        this.degree = degree;
    }

    public int degree() {
        return this.degree;
    }

    public int decreaseDegree(int decrease) {
        assert decrease <= this.degree;
        this.degree -= decrease;
        return this.degree;
    }

    public boolean active() {
        return this.degree > 0;
    }

    public void core(Id core) {
        this.core = core;
    }

    public Id core() {
        E.checkNotNull(this.core, "core");
        return this.core;
    }

    @Override
    public KcoreValue copy() {
        KcoreValue kcoreValue = new KcoreValue();
        kcoreValue.core = (Id) this.core.copy();
        kcoreValue.degree = this.degree;
        return kcoreValue;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.core.read(in);
        this.degree = in.readInt();
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.core.write(out);
        out.writeInt(this.degree);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                   .append("core", this.core)
                   .append("degree", this.degree)
                   .toString();
    }

    @Override
    public Object value() {
        return this.core.value();
    }
}
