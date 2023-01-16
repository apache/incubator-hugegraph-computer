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

package org.apache.hugegraph.computer.core.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.SerialEnum;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.graph.edge.DefaultEdge;
import org.apache.hugegraph.computer.core.graph.edge.DefaultEdges;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.DefaultProperties;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.BooleanValue;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.FloatValue;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.graph.value.IdSet;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.graph.value.ListValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.MapValue;
import org.apache.hugegraph.computer.core.graph.value.NullValue;
import org.apache.hugegraph.computer.core.graph.value.StringValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.graph.vertex.DefaultVertex;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;

public final class BuiltinGraphFactory implements GraphFactory {

    private static final int AVERAGE_DEGREE = 10;

    @Override
    public Id createId() {
        return new BytesId();
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
    public <V extends Value> Vertex createVertex(Id id, V value) {
        return new DefaultVertex(this, id, value);
    }

    @Override
    public <V extends Value> Vertex createVertex(String label, Id id, V value) {
        return new DefaultVertex(this, label, id, value);
    }

    @Override
    public Edges createEdges() {
        return this.createEdges(AVERAGE_DEGREE);
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
        return new DefaultEdge(this, Constants.EMPTY_STR,
                               Constants.EMPTY_STR, targetId);
    }

    @Override
    public Edge createEdge(String label, Id targetId) {
        return new DefaultEdge(this, label, Constants.EMPTY_STR, targetId);
    }

    @Override
    public Edge createEdge(String label, String name, Id targetId) {
        return new DefaultEdge(this, label, name, targetId);
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
    public <V> Set<V> createSet() {
        return new HashSet<>();
    }

    @Override
    public <V> Set<V> createSet(int capacity) {
        return new HashSet<>(capacity);
    }

    @Override
    public <K, V> Map<K, V> createMap() {
        return new HashMap<>();
    }

    @Override
    public Properties createProperties() {
        return new DefaultProperties(this);
    }

    @Override
    public Value createValue(byte code) {
        ValueType type = SerialEnum.fromCode(ValueType.class, code);
        return createValue(type);
    }

    /**
     * Create property value by type.
     */
    @Override
    public Value createValue(ValueType type) {
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
            case ID:
                return new BytesId();
            case ID_LIST:
                return new IdList();
            case ID_LIST_LIST:
                return new IdListList();
            case ID_SET:
                return new IdSet();
            case LIST_VALUE:
                return new ListValue<>();
            case MAP_VALUE:
                return new MapValue<>();
            case STRING:
                return new StringValue();
            default:
                throw new ComputerException("Can't create Value for %s",
                                            type.name());
        }
    }
}
