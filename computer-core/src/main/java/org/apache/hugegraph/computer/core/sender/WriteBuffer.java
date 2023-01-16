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

package org.apache.hugegraph.computer.core.sender;

import java.io.IOException;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.GraphComputeOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.StreamGraphOutput;
import org.apache.hugegraph.computer.core.store.entry.EntryOutput;
import org.apache.hugegraph.computer.core.store.entry.EntryOutputImpl;

/**
 * It's not a public class, need package access
 */
class WriteBuffer {

    /*
     * When writed bytes exceed this threshold, means that need a new buffer
     * to continue write
     */
    private final int threshold;
    private final BytesOutput bytesOutput;
    private final GraphComputeOutput graphOutput;
    private long writeCount;

    public WriteBuffer(ComputerContext context, int threshold, int capacity) {
        assert threshold > 0 && capacity > 0 && threshold <= capacity;
        this.threshold = threshold;
        this.bytesOutput = IOFactory.createBytesOutput(capacity);
        EntryOutput entryOutput = new EntryOutputImpl(this.bytesOutput);
        this.graphOutput = new StreamGraphOutput(context, entryOutput);
        this.writeCount = 0L;
    }

    public boolean reachThreshold() {
        return this.bytesOutput.position() >= this.threshold;
    }

    public boolean isEmpty() {
        return this.bytesOutput.position() == 0L;
    }

    public long numBytes() {
        return this.bytesOutput.position();
    }

    public long writeCount() {
        return this.writeCount;
    }

    public void clear() throws IOException {
        this.writeCount = 0L;
        this.bytesOutput.seek(0L);
    }

    public BytesOutput output() {
        return this.bytesOutput;
    }

    public void writeVertex(Vertex vertex) throws IOException {
        this.writeCount++;
        this.graphOutput.writeVertex(vertex);
    }

    public void writeEdges(Vertex vertex) throws IOException {
        this.writeCount++;
        this.graphOutput.writeEdges(vertex);
    }

    public void writeMessage(Id targetId, Value value) throws IOException {
        this.writeCount++;
        this.graphOutput.writeMessage(targetId, value);
    }
}
