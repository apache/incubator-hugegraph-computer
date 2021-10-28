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

package com.baidu.hugegraph.computer.algorithm.path.links;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.DefaultProperties;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class LinksMessage implements Value<LinksMessage> {

    private IdList pathVertexes;
    private IdList pathEdges;
    private Properties walkEdgeProps;

    public LinksMessage() {
        GraphFactory graphFactory = ComputerContext.instance().graphFactory();
        this.pathVertexes = new IdList();
        this.pathEdges = new IdList();
        this.walkEdgeProps = new DefaultProperties(graphFactory);
    }

    @Override
    public ValueType valueType() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<LinksMessage> other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(LinksMessage o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.pathVertexes.read(in);
        this.pathEdges.read(in);
        this.walkEdgeProps.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.pathVertexes.write(out);
        this.pathEdges.write(out);
        this.walkEdgeProps.write(out);
    }

    @Override
    public LinksMessage copy() {
        LinksMessage message = new LinksMessage();
        message.pathVertexes = this.pathVertexes.copy();
        message.pathEdges = this.pathEdges.copy();
        message.walkEdgeProps = this.walkEdgeProps;
        return message;
    }

    @Override
    public Object value() {
        throw new UnsupportedOperationException();
    }

    public IdList pathVertexes() {
        return this.pathVertexes;
    }

    public IdList pathEdge() {
        return this.pathEdges;
    }

    public Properties walkEdgeProp() {
        return this.walkEdgeProps;
    }

    public void walkEdgeProp(Properties properties) {
        this.walkEdgeProps = properties;
    }

    public void addVertex(Id vertexId) {
        this.pathVertexes.add(vertexId);
    }

    public void addEdge(Id edgeId) {
        this.pathEdges.add(edgeId);
    }
}
