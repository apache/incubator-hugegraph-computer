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

package com.baidu.hugegraph.computer.core.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdge;
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdges;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.id.Utf8Id;
import com.baidu.hugegraph.computer.core.graph.id.UuidId;
import com.baidu.hugegraph.computer.core.graph.properties.DefaultProperties;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactory;
import com.baidu.hugegraph.computer.core.graph.vertex.DefaultVertex;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public final class BuiltinGraphFactory implements GraphFactory {

    private final Config config;
    private ValueFactory valueFactory;

    public BuiltinGraphFactory(Config config, ValueFactory valueFactory) {
        this.config = config;
        this.valueFactory = valueFactory;
    }

    @Override
    public Id createId(long id) {
        return new LongId(id);
    }

    @Override
    public Id createId(String id) {
        return new Utf8Id(id);
    }

    @Override
    public Id createId(UUID id) {
        return new UuidId(id);
    }

    @Override
    public Vertex createVertex() {
        return new DefaultVertex(this);
    }

    @Override
    public <V extends Value<?>> Vertex createVertex(Id id, V value) {
        return new DefaultVertex(this, id, value);
    }

    @Override
    public Edges createEdges() {
        int averageDegree = this.config.get(
                            ComputerOptions.VERTEX_AVERAGE_DEGREE);
        return createEdges(averageDegree);
    }

    @Override
    public Edges createEdges(int capacity) {
        return new DefaultEdges(this, capacity);
    }

    @Override
    public Edge createEdge() {
        return new DefaultEdge(this);
    }

    @Override
    public Edge createEdge(Id targetId) {
        return new DefaultEdge(this, targetId, null, null);
    }

    @Override
    public Edge createEdge(Id targetId, String label) {
        return new DefaultEdge(this, targetId, label, null);
    }

    @Override
    public Edge createEdge(Id targetId, String label, String name) {
        return new DefaultEdge(this, targetId, label, name);
    }

    @Override
    public <V> List<V> createList() {
        return new ArrayList<>();
    }

    @Override
    public <V> List<V> createList(int capacity) {
        return new ArrayList<>(capacity);
    }

    @Override
    public <K, V> Map<K, V> createMap() {
        return new HashMap<>();
    }

    @Override
    public Properties createProperties() {
        return new DefaultProperties(this, this.valueFactory);
    }
}
