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
import java.util.concurrent.atomic.AtomicReference;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.EdgeFrequency;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdFactory;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactory;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryInput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntryReader;

import javafx.util.Pair;

public class StreamGraphInput implements GraphComputeInput {

    private final GraphFactory graphFactory;
    private final ValueFactory valueFactory;
    private final EdgeFrequency frequency;
    private final EntryInput in;

    public StreamGraphInput(ComputerContext context, EntryInput in) {
        this.graphFactory = context.graphFactory();
        this.valueFactory = context.valueFactory();
        this.frequency = context.config().get(ComputerOptions.INPUT_EDGE_FREQ);
        this.in = in;
    }

    @Override
    public Vertex readVertex() throws IOException {
        Vertex vertex = this.graphFactory.createVertex();
        this.in.readEntry(in -> {
            vertex.id(this.readId(in));
        }, in -> {
            vertex.properties(this.readProperties(in));
        });
        return vertex;
    }

    @Override
    public Vertex readEdges() throws IOException {
        Vertex vertex = this.graphFactory.createVertex();
        KvEntryReader reader = this.in.readEntry(in -> {
            // Read id
            vertex.id(this.readId(in));
        });
        if (this.frequency == EdgeFrequency.SINGLE) {
            while (reader.hasRemaining()) {
                Edge edge = this.graphFactory.createEdge();
                // Only use targetId as subKey, use properties as subValue
                reader.readSubKv(in -> {
                    edge.targetId(this.readId(in));
                }, in -> {
                    edge.properties(this.readProperties(in));
                });
                vertex.addEdge(edge);
            }
        } else if (frequency == EdgeFrequency.SINGLE_PER_LABEL) {
            while (reader.hasRemaining()) {
                Edge edge = this.graphFactory.createEdge();
                // Use label + targetId as subKey, use properties as subValue
                reader.readSubKv(in -> {
                    edge.label(in.readUTF());
                    edge.targetId(this.readId(in));
                }, in -> {
                    edge.properties(this.readProperties(in));
                });
                vertex.addEdge(edge);
            }
        } else {
            assert frequency == EdgeFrequency.MULTIPLE;
            while (reader.hasRemaining()) {
                Edge edge = this.graphFactory.createEdge();
                /*
                 * Use label + sortValues + targetId as subKey,
                 * use properties as subValue
                 */
                reader.readSubKv(in -> {
                    edge.label(in.readUTF());
                    edge.name(in.readUTF());
                    edge.targetId(this.readId(in));
                }, in -> {
                    edge.properties(this.readProperties(in));
                });
                vertex.addEdge(edge);
            }
        }
        return vertex;
    }

    @Override
    public Pair<Id, Value<?>> readMessage() throws IOException {
        AtomicReference<Id> idRef = new AtomicReference<>();
        AtomicReference<Value<?>> valueRef = new AtomicReference<>();
        this.in.readEntry(in -> {
            // Read id
            idRef.set(this.readId(in));
        }, in -> {
            valueRef.set(this.readValue(in));
        });
        return new Pair<>(idRef.get(), valueRef.get());
    }

    private Id readId(RandomAccessInput in) throws IOException {
        byte code = in.readByte();
        Id id = IdFactory.createId(code);
        id.read(in);
        return id;
    }

    private Value<?> readValue(RandomAccessInput in) throws IOException {
        byte code = in.readByte();
        Value<?> value = this.valueFactory.createValue(code);
        value.read(in);
        return value;
    }

    private Properties readProperties(RandomAccessInput in) throws IOException {
        Properties properties = this.graphFactory.createProperties();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = in.readUTF();
            Value<?> value = this.readValue(in);
            properties.put(key, value);
        }
        return properties;
    }
}
