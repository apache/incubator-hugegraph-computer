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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public interface GraphFactory {

    Id createId(long id);

    Id createId(String id);

    Id createId(UUID id);

    Vertex createVertex();

    <V extends Value<?>> Vertex createVertex(Id id, V value);

    Edges createEdges();

    Edges createEdges(int capacity);

    Edge createEdge();

    Edge createEdge(Id targetId);

    Edge createEdge(Id targetId, String label);

    Edge createEdge(Id targetId, String label, String name);

    Properties createProperties();

    <V> List<V> createList();

    <V> List<V> createList(int capacity);

    <K, V> Map<K, V> createMap();
}
