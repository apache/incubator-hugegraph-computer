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

package com.baidu.hugegraph.computer.core.graph.vertex;

import com.baidu.hugegraph.computer.core.allocator.Recyclable;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.Value;

public interface Vertex extends Recyclable {

    String label();

    void label(String label);

    Id id();

    void id(Id id);

    <V extends Value<?>> V value();

    <V extends Value<?>> void value(V value);

    int numEdges();

    Edges edges();

    void edges(Edges edges);

    void addEdge(Edge edge);

    Properties properties();

    void properties(Properties properties);

    <T extends Value<T>> T property(String key);

    boolean active();

    void inactivate();

    void reactivate();
}
