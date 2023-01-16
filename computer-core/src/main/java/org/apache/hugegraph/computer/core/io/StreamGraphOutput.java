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

package org.apache.hugegraph.computer.core.io;

import java.io.IOException;
import java.util.Map;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.EdgeFrequency;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.store.entry.EntryOutput;
import org.apache.hugegraph.computer.core.store.entry.KvEntryWriter;

public class StreamGraphOutput implements GraphComputeOutput {

    private final EntryOutput out;
    private final EdgeFrequency frequency;

    public StreamGraphOutput(ComputerContext context, EntryOutput out) {
        this.out = out;
        this.frequency = context.config().get(ComputerOptions.INPUT_EDGE_FREQ);
    }

    @Override
    public void writeVertex(Vertex vertex) throws IOException {
        this.out.writeEntry(out -> {
            // Write id
            this.writeId(out, vertex.id());
        }, out -> {
            // Write label
            this.writeLabel(out, vertex.label());
            // Write properties
            this.writeProperties(out, vertex.properties());
        });
    }

    @Override
    public void writeEdges(Vertex vertex) throws IOException {
        KvEntryWriter writer = this.out.writeEntry(out -> {
            // Write id
            this.writeId(out, vertex.id());
        });
        if (this.frequency == EdgeFrequency.SINGLE) {
            for (Edge edge : vertex.edges()) {
                // Only use targetId as subKey, use properties as subValue
                writer.writeSubKv(out -> {
                    this.writeId(out, edge.targetId());
                }, out -> {
                    this.writeProperties(out, edge.properties());
                });
            }
        } else if (this.frequency == EdgeFrequency.SINGLE_PER_LABEL) {
            for (Edge edge : vertex.edges()) {
                // Use label + targetId as subKey, use properties as subValue
                writer.writeSubKv(out -> {
                    this.writeLabel(out, edge.label());
                    this.writeId(out, edge.targetId());
                }, out -> {
                    this.writeProperties(out, edge.properties());
                });
            }
        } else {
            assert this.frequency == EdgeFrequency.MULTIPLE;
            for (Edge edge : vertex.edges()) {
                /*
                 * Use label + sortValues + targetId as subKey,
                 * use properties as subValue
                 */
                writer.writeSubKv(out -> {
                    this.writeLabel(out, edge.label());
                    this.writeLabel(out, edge.name());
                    this.writeId(out, edge.targetId());
                }, out -> {
                    this.writeProperties(out, edge.properties());
                });
            }
        }
        writer.writeFinish();
    }

    @Override
    public void writeMessage(Id id, Value value) throws IOException {
        this.out.writeEntry(out -> {
            // Write id
            this.writeId(out, id);
        }, out -> {
            this.writeMessage(out, value);
        });
    }

    @Override
    public void writeId(RandomAccessOutput out, Id id) throws IOException {
        id.write(out);
    }

    @Override
    public void writeValue(RandomAccessOutput out, Value value)
                           throws IOException {
        out.writeByte(value.valueType().code());
        value.write(out);
    }

    private void writeMessage(RandomAccessOutput out, Value value)
                              throws IOException {
        value.write(out);
    }

    private void writeProperties(RandomAccessOutput out, Properties properties)
                                 throws IOException {
        Map<String, Value> keyValues = properties.get();
        out.writeInt(keyValues.size());
        for (Map.Entry<String, Value> entry : keyValues.entrySet()) {
            out.writeUTF(entry.getKey());
            this.writeValue(out, entry.getValue());
        }
    }

    private void writeLabel(RandomAccessOutput output, String label)
                            throws IOException {
        output.writeUTF(label);
    }
}
