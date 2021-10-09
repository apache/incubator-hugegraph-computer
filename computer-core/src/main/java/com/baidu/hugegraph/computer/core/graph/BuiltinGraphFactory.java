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

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.common.SerialEnum;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdge;
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdges;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.DefaultProperties;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValue;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.FloatValue;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.ListValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.value.NullValue;
import com.baidu.hugegraph.computer.core.graph.value.StringValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.graph.vertex.DefaultVertex;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public final class BuiltinGraphFactory implements GraphFactory {

    private final Config config;

    public BuiltinGraphFactory(Config config) {
        this.config = config;
    }

    @Override
    public Id createId(long id) {
        return BytesId.of(id);
    }

    @Override
    public Id createId(String id) {
        return BytesId.of(id);
    }

    @Override
    public Id createId(UUID id) {
        return BytesId.of(id);
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
    public <V extends Value<?>> Vertex createVertex(String label, Id id,
                                                    V value) {
        return new DefaultVertex(this, label, id, value);
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
        return new DefaultEdge(this, null, Constants.EMPTY_STR,
                               Constants.EMPTY_STR, targetId);
    }

    @Override
    public Edge createEdge(String label, Id targetId) {
        return new DefaultEdge(this, null, label, Constants.EMPTY_STR,
                               targetId);
    }

    @Override
    public Edge createEdge(String label, String name, Id targetId) {
        return new DefaultEdge(this, null, label, name, targetId);
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
        return new DefaultProperties(this);
    }

    public Value<?> createValue(byte code) {
        ValueType type = SerialEnum.fromCode(ValueType.class, code);
        return createValue(type);
    }

    /**
     * Create property value by type.
     */
    public Value<?> createValue(ValueType type) {
        switch (type) {
            case NULL:
                return NullValue.get();
            case BOOLEAN:
                return new BooleanValue();
            case INT:
                return new IntValue();
            case LONG:
                return new LongValue();
            case FLOAT:
                return new FloatValue();
            case DOUBLE:
                return new DoubleValue();
            case ID_VALUE:
                return new BytesId();
            case ID_VALUE_LIST:
                return new IdList();
            case ID_VALUE_LIST_LIST:
                return new IdListList();
            case LIST_VALUE:
                return new ListValue<>();
            case STRING:
                return new StringValue();
            default:
                throw new ComputerException("Can't create Value for %s",
                                            type.name());
        }
    }
}
