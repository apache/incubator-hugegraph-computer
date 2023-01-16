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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.value.ValueType;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;

public interface GraphFactory {

    Id createId();

    Id createId(long id);

    Id createId(String id);

    Id createId(UUID id);

    Vertex createVertex();

    <V extends Value> Vertex createVertex(Id id, V value);

    <V extends Value> Vertex createVertex(String label, Id id, V value);

    Edges createEdges();

    Edges createEdges(int capacity);

    Edge createEdge();

    Edge createEdge(Id targetId);

    Edge createEdge(String label, Id targetId);

    Edge createEdge(String label, String name, Id targetId);

    Properties createProperties();

    <V> List<V> createList();

    <V> List<V> createList(int capacity);

    <V> Set<V> createSet();

    <V> Set<V> createSet(int size);

    <K, V> Map<K, V> createMap();

    Value createValue(byte code);

    Value createValue(ValueType type);
}
