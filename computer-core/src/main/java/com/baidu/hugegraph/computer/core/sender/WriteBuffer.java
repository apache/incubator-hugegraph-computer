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

package com.baidu.hugegraph.computer.core.sender;

import java.io.IOException;
import java.util.Map;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.EdgeFrequency;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeBytesOutput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutputImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntryWriter;

/**
 * It's not a public class
 */
class WriteBuffer {

    /*
     * When writed bytes exceed this threshold, means that need a new buffer
     * to continue write
     */
    private final int threshold;
    private final OptimizedUnsafeBytesOutput output;
    private final EntryOutput entryOutput;

    public WriteBuffer(int threshold, int capacity) {
        assert threshold > 0 && capacity > 0 && threshold <= capacity;
        this.threshold = threshold;
        this.output = new OptimizedUnsafeBytesOutput(capacity);
        this.entryOutput = new EntryOutputImpl(this.output);
    }

    public boolean reachThreshold() {
        return this.output.position() >= this.threshold;
    }

    public boolean isEmpty() {
        return this.output.position() == 0L;
    }

    public void clear() {
        try {
            this.output.seek(0L);
        } catch (IOException e) {
            throw new ComputerException("Failed to seek to position 0");
        }
    }

    public OptimizedUnsafeBytesOutput output() {
        return this.output;
    }

    public void writeVertex(Vertex vertex) throws IOException {
        this.entryOutput.writeEntry(vertex.id(), out -> {
            // write properties
            this.writeProperties(out, vertex.properties());
        });
    }

    public void writeEdge(Vertex vertex) throws IOException {
        EdgeFrequency frequency = ComputerContext.instance().config().get(
                                  ComputerOptions.INPUT_EDGE_FREQ);
        KvEntryWriter writer = this.entryOutput.writeEntry(vertex.id());
        if (frequency == EdgeFrequency.SINGLE) {
            for (Edge edge : vertex.edges()) {
                // Only use targetId as subKey, use properties as subValue
                writer.writeSubKv(edge.targetId(), out -> {
                    this.writeProperties(out, edge.properties());
                });
            }
        } else if (frequency == EdgeFrequency.SINGLE_PER_LABEL) {
            for (Edge edge : vertex.edges()) {
                // Use label + targetId as subKey, use properties as subValue
                writer.writeSubKv(out -> {
                    out.writeUTF(edge.label());
                    out.writeByte(edge.targetId().type().code());
                    edge.targetId().write(out);
                }, out -> {
                    this.writeProperties(out, edge.properties());
                });
            }
        } else {
            assert frequency == EdgeFrequency.MULTIPLE;
            for (Edge edge : vertex.edges()) {
                /*
                 * Use label + sortValues + targetId as subKey,
                 * use properties as subValue
                 */
                writer.writeSubKv(out -> {
                    out.writeUTF(edge.label());
                    out.writeUTF(edge.name());
                    out.writeByte(edge.targetId().type().code());
                    edge.targetId().write(out);
                }, out -> {
                    this.writeProperties(out, edge.properties());
                });
            }
        }
        writer.writeFinish();
    }

    public void writeMessage(Id targetId, Value<?> value) throws IOException {
        this.entryOutput.writeEntry(targetId, value);
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
