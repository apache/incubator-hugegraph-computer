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

package org.apache.hugegraph.computer.algorithm.centrality.closeness;

import java.io.IOException;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.id.IdFactory;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.Value.CustomizeValue;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;

public class ClosenessMessage implements CustomizeValue<ClosenessMessage> {

    private final GraphFactory graphFactory;

    private Id senderId;
    private Id startId;
    private DoubleValue distance;

    public ClosenessMessage() {
        this(IdFactory.createId(), IdFactory.createId(), new DoubleValue(0.0D));
    }

    public ClosenessMessage(Id senderId, Id startId, DoubleValue distance) {
        this.graphFactory = ComputerContext.instance().graphFactory();
        this.senderId = senderId;
        this.startId = startId;
        this.distance = distance;
    }

    public Id senderId() {
        return this.senderId;
    }

    public Id startId() {
        return this.startId;
    }

    public DoubleValue distance() {
        return this.distance;
    }

    @Override
    public ClosenessMessage value() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.senderId = this.graphFactory.createId();
        this.senderId.read(in);

        this.startId = this.graphFactory.createId();
        this.startId.read(in);

        this.distance = (DoubleValue) this.graphFactory.createValue(
                                      ValueType.DOUBLE);
        this.distance.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.senderId.write(out);
        this.startId.write(out);
        this.distance.write(out);
    }
}
