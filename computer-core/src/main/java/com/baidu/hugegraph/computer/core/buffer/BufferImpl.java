/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.computer.core.buffer;

import java.io.IOException;
import java.util.Map;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.EdgeFrequency;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.io.EntryOutput;
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.store.hghvfile.entry.EntryOutputImpl;
import com.baidu.hugegraph.computer.core.store.hghvfile.entry.KvEntryWriter;

public class BufferImpl {

    private final int bufferSize;
    private final RandomAccessOutput output;
    private final EntryOutput entryOutput;

    public BufferImpl(int size, int capacity) {
        this.bufferSize = size;
        this.output = new OptimizedUnsafeBytesOutput(capacity);
        this.entryOutput = new EntryOutputImpl(this.output);
    }

    public boolean reachThreshold() {
        return this.output.position() >= this.bufferSize;
    }

    public boolean isEmpty() {
        return this.output.position() == 0L;
    }

    public void clear() {
        try {
            this.output.seek(0L);
        } catch (IOException e) {
            throw new ComputerException("Failed to seek to byte 0 point");
        }
    }

    public RandomAccessOutput output() {
        return this.output;
    }

    public void writeVertex(MessageType type, Vertex vertex)
                            throws IOException {
        if (type != MessageType.VERTEX && type != MessageType.EDGE) {
            throw new ComputerException("Unexpected MessageType=%s", type);
        }
        if (type == MessageType.VERTEX) {
            this.writeVertexWithProperties(vertex);
        } else {
            this.writeVertexWithEdges(vertex);
        }
    }

    private void writeVertexWithProperties(Vertex vertex) throws IOException {
        this.entryOutput.writeEntry(vertex.id(), out -> {
            // write properties
            this.writeProperties(out, vertex.properties());
        });
    }

    private void writeVertexWithEdges(Vertex vertex) throws IOException {
        EdgeFrequency frequency = ComputerContext.instance().config().get(
                                  ComputerOptions.EDGE_FREQ_IN_VERTEX_PAIR);
        KvEntryWriter writer = this.entryOutput.writeEntry(vertex.id());
        if (frequency == EdgeFrequency.SINGLE) {
            for (Edge edge : vertex.edges()) {
                // Only use targetId as subKey
                writer.writeSubKey(edge.targetId());
                // Use properties as subValue
                writer.writeSubValue(out -> {
                    this.writeProperties(out, edge.properties());
                });
            }
        } else if (frequency == EdgeFrequency.SINGLE_PER_LABEL) {
            for (Edge edge : vertex.edges()) {
                // Use label + targetId as subKey
                writer.writeSubKey(out -> {
                    out.writeUTF(edge.label());
                    out.writeByte(edge.targetId().type().code());
                    edge.targetId().write(out);
                });
                // Use properties as subValue
                writer.writeSubValue(out -> {
                    this.writeProperties(out, edge.properties());
                });
            }
        } else {
            assert frequency == EdgeFrequency.MULTI;
            for (Edge edge : vertex.edges()) {
                // Use label + sortValues + targetId as subKey
                writer.writeSubKey(out -> {
                    out.writeUTF(edge.label());
                    out.writeUTF(edge.name());
                    out.writeByte(edge.targetId().type().code());
                    edge.targetId().write(out);
                });
                // Use properties as subValue
                writer.writeSubValue(out -> {
                    this.writeProperties(out, edge.properties());
                });
            }
        }
        writer.writeFinish();
    }

    private void writeProperties(RandomAccessOutput out, Properties properties)
                                 throws IOException {
        Map<String, Value<?>> keyValues = properties.get();
        out.writeInt(keyValues.size());
        for (Map.Entry<String, Value<?>> entry : keyValues.entrySet()) {
            out.writeUTF(entry.getKey());
            Value<?> value = entry.getValue();
            out.writeByte(value.type().code());
            value.write(out);
        }
    }
}
