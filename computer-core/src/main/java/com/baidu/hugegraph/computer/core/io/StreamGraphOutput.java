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

package com.baidu.hugegraph.computer.core.io;

import java.io.IOException;
import java.util.Map;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.config.EdgeFrequency;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutputImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntryWriter;

public class StreamGraphOutput implements GraphComputeOutput {

    private final EntryOutput out;

    public StreamGraphOutput(ComputerContext context, RandomAccessOutput out) {
        this(context, new EntryOutputImpl(out));
    }

    public StreamGraphOutput(ComputerContext context, EntryOutput out) {
        this.out = out;
    }

    @Override
    public void writeVertex(Vertex vertex) throws IOException {
        this.out.writeEntry(out -> {
            // Write id
            out.writeByte(vertex.id().type().code());
            vertex.id().write(out);
        }, out -> {
            // Write properties
            this.writeProperties(out, vertex.properties());
        });
    }

    @Override
    public void writeEdges(Vertex vertex) throws IOException {
        Config config = ComputerContext.instance().config();
        EdgeFrequency frequency = config.get(ComputerOptions.INPUT_EDGE_FREQ);
        KvEntryWriter writer = this.out.writeEntry(out -> {
            // Write id
            out.writeByte(vertex.id().type().code());
            vertex.id().write(out);
        });
        if (frequency == EdgeFrequency.SINGLE) {
            for (Edge edge : vertex.edges()) {
                // Only use targetId as subKey, use properties as subValue
                writer.writeSubKv(out -> {
                    out.writeByte(edge.targetId().type().code());
                    edge.targetId().write(out);
                }, out -> {
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

    @Override
    public void writeMessage(Id id, Value<?> value) throws IOException {
        this.out.writeEntry(out -> {
            // Write id
            out.writeByte(id.type().code());
            id.write(out);
        }, out -> {
            value.write(out);
        });
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
