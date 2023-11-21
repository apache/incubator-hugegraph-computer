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
import org.apache.hugegraph.computer.core.graph.value.ListValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;

public class ShortestPathMessage implements Value.CustomizeValue<List<Object>> {

    /**
     * path
     */
    private final IdList path;

    /**
     * weight of path
     */
    private final ListValue<DoubleValue> pathWeight;

    public ShortestPathMessage() {
        this.path = new IdList();
        this.pathWeight = new ListValue<DoubleValue>();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.path.read(in);
        this.pathWeight.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.path.write(out);
        this.pathWeight.write(out);
    }

    @Override
    public List<Object> value() {
        return this.path.value();
    }

    public void value(ShortestPathMessage message) {
        this.path.clear();
        this.pathWeight.clear();

        this.path.addAll(message.path().copy().values());
        this.pathWeight.addAll(message.pathWeight().copy().values());
    }

    public void addToPath(Vertex vertex, double weight) {
        this.path.add(vertex.id());
        this.pathWeight.add(new DoubleValue(weight));
    }

    public void addToPath(IdList path, ListValue<DoubleValue> pathWeight,
                          Vertex vertex, double weight) {
        this.path.addAll(path.copy().values());
        this.pathWeight.addAll(pathWeight.copy().values());

        this.path.add(vertex.id());
        this.pathWeight.add(new DoubleValue(weight));
    }

    public IdList path() {
        return this.path;
    }

    public ListValue<DoubleValue> pathWeight() {
        return this.pathWeight;
    }

    public double totalWeight() {
        double totalWeight = 0;
        for (int i = 0; i < this.pathWeight.size(); ++i) {
            totalWeight += this.pathWeight.get(i).doubleValue();
        }
        return totalWeight;
    }
}
