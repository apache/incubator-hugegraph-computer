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

import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdge;
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdges;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.DefaultVertex;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public final class GraphFactory {

    public Vertex createVertex() {
        return new DefaultVertex();
    }

    public <V extends Value<?>> Vertex createVertex(Id id, V value) {
        return new DefaultVertex(id, value);
    }

    public Edges createEdges(int capacity) {
        return new DefaultEdges(capacity);
    }

    public Edge createEdge() {
        return new DefaultEdge();
    }

    public <V extends Value<?>> Edge createEdge(Id targetId, V value) {
        return new DefaultEdge(targetId, value);
    }

    public <V> List<V> createList() {
        return new ArrayList<>();
    }

    public <V> List<V> createList(int capacity) {
        return new ArrayList<>(capacity);
    }

    public <K, V> Map<K, V> createMap() {
        return new HashMap<>();
    }
}
