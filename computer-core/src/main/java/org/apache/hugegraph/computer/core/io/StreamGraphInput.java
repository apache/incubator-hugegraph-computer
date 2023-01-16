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

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.config.EdgeFrequency;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.store.entry.EntryInput;
import org.apache.hugegraph.computer.core.store.entry.KvEntryReader;

public class StreamGraphInput implements GraphComputeInput {

    private final GraphFactory graphFactory;
    private final Config config;
    private final EdgeFrequency frequency;
    private final EntryInput in;

    public StreamGraphInput(ComputerContext context, EntryInput in) {
        this.graphFactory = context.graphFactory();
        this.config = context.config();
        this.frequency = context.config().get(ComputerOptions.INPUT_EDGE_FREQ);
        this.in = in;
    }

    @Override
    public Vertex readVertex() throws IOException {
        Vertex vertex = this.graphFactory.createVertex();
        this.in.readEntry(in -> {
            vertex.id(readId(in));
        }, in -> {
            vertex.label(readLabel(in));
            vertex.properties(readProperties(in));
        });
        return vertex;
    }

    @Override
    public Vertex readEdges() throws IOException {
        Vertex vertex = this.graphFactory.createVertex();
        KvEntryReader reader = this.in.readEntry(in -> {
            // Read id
            vertex.id(readId(in));
        });
        if (this.frequency == EdgeFrequency.SINGLE) {
            while (reader.hasRemaining()) {
                Edge edge = this.graphFactory.createEdge();
                // Only use targetId as subKey, use properties as subValue
                reader.readSubKv(in -> {
                    edge.targetId(readId(in));
                }, in -> {
                    edge.properties(readProperties(in));
                });
                vertex.addEdge(edge);
            }
        } else if (this.frequency == EdgeFrequency.SINGLE_PER_LABEL) {
            while (reader.hasRemaining()) {
                Edge edge = this.graphFactory.createEdge();
                // Use label + targetId as subKey, use properties as subValue
                reader.readSubKv(in -> {
                    edge.label(readLabel(in));
                    edge.targetId(readId(in));
                }, in -> {
                    edge.properties(readProperties(in));
                });
                vertex.addEdge(edge);
            }
        } else {
            assert this.frequency == EdgeFrequency.MULTIPLE;
            while (reader.hasRemaining()) {
                Edge edge = this.graphFactory.createEdge();
                /*
                 * Use label + sortValues + targetId as subKey,
                 * use properties as subValue
                 */
                reader.readSubKv(in -> {
                    edge.label(readLabel(in));
                    edge.name(readLabel(in));
                    edge.targetId(readId(in));
                }, in -> {
                    edge.properties(this.readProperties(in));
                });
                vertex.addEdge(edge);
            }
        }
        return vertex;
    }

    @Override
    public Pair<Id, Value> readMessage() throws IOException {
        MutablePair<Id, Value> pair = MutablePair.of(null, null);
        this.in.readEntry(in -> {
            // Read id
            pair.setLeft(readId(in));
        }, in -> {
            pair.setRight(this.readMessage(in));
        });
        return pair;
    }

    private Value readMessage(RandomAccessInput in) throws IOException {
        Value value = this.config.createObject(
                      ComputerOptions.ALGORITHM_MESSAGE_CLASS);
        value.read(in);
        return value;
    }

    @Override
    public Value readValue(RandomAccessInput in) throws IOException {
        byte code = in.readByte();
        Value value = this.graphFactory.createValue(code);
        value.read(in);
        return value;
    }

    private Properties readProperties(RandomAccessInput in) throws IOException {
        Properties properties = this.graphFactory.createProperties();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            Value value = this.readValue(in);
            properties.put(key, value);
        }
        return properties;
    }

    public static Id readId(RandomAccessInput in) throws IOException {
        Id id = new BytesId();
        id.read(in);
        return id;
    }

    public static String readLabel(RandomAccessInput in) throws IOException {
        return in.readUTF();
    }
}
