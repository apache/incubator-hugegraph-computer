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

package org.apache.hugegraph.computer.algorithm.path.shortest;

import java.io.IOException;
import java.util.List;

import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;

public class SingleSourceShortestPathValue implements Value.CustomizeValue<List<Object>> {

    /**
     * path
     */
    private final IdList path;

    /**
     * total weight.
     * infinity means unreachable.
     */
    private final DoubleValue totalWeight;

    public SingleSourceShortestPathValue() {
        this.path = new IdList();
        this.totalWeight = new DoubleValue(0);
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.path.read(in);
        this.totalWeight.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.path.write(out);
        this.totalWeight.write(out);
    }

    @Override
    public List<Object> value() {
        return this.path.value();
    }

    public void unreachable() {
        this.totalWeight.value(Double.POSITIVE_INFINITY);
    }

    public void zeroDistance() {
        this.totalWeight.value(0);
    }

    public void shorterPath(Vertex vertex, IdList path, double weight) {
        this.path.clear();
        this.path.addAll(path.copy().values());
        this.path.add(vertex.id());
        this.totalWeight.value(weight);
    }

    public void addToPath(Vertex vertex, double weight) {
        this.path.add(vertex.id());
        this.totalWeight.value(weight);
    }

    public void addToPath(IdList path, double weight) {
        this.path.addAll(path.copy().values());
        this.totalWeight.value(weight);
    }

    public IdList path() {
        return this.path;
    }

    public double totalWeight() {
        return this.totalWeight.doubleValue();
    }

    public void copy(SingleSourceShortestPathValue value) {
        this.path.clear();
        this.path.addAll(value.path().copy().values());
        this.totalWeight.value(value.totalWeight());
    }
}
