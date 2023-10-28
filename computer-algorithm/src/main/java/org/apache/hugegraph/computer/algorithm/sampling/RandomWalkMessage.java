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

package org.apache.hugegraph.computer.algorithm.sampling;

import org.apache.hugegraph.computer.core.graph.value.BooleanValue;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;

import java.io.IOException;
import java.util.List;

public class RandomWalkMessage implements Value.CustomizeValue<List<Object>> {

    /**
     * random walk path
     */
    private final IdList path;

    /**
     * finish flag
     */
    private BooleanValue isFinish;

    public RandomWalkMessage() {
        this.path = new IdList();
        this.isFinish = new BooleanValue(false);
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.path.read(in);
        this.isFinish.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.path.write(out);
        this.isFinish.write(out);
    }

    @Override
    public List<Object> value() {
        return this.path.value();
    }

    public IdList path() {
        return this.path;
    }

    public void addToPath(Vertex vertex) {
        this.path.add(vertex.id());
    }

    public boolean isFinish() {
        return this.isFinish.boolValue();
    }

    public void finish() {
        this.isFinish = new BooleanValue(true);
    }
}
