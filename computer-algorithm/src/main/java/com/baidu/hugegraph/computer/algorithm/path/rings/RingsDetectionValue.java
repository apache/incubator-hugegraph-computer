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

package com.baidu.hugegraph.computer.algorithm.path.rings;

import java.io.IOException;

import javax.ws.rs.NotSupportedException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.properties.DefaultProperties;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class RingsDetectionValue implements Value<RingsDetectionValue> {

    private static final GraphFactory FACTORY = ComputerContext.instance()
                                                               .graphFactory();

    private final IdList path;
    private Properties inEdgeProp;

    public RingsDetectionValue() {
        this.path = new IdList();
        this.inEdgeProp = new DefaultProperties(FACTORY);
    }

    @Override
    public ValueType type() {
        return ValueType.UNKNOWN;
    }

    @Override
    public void assign(Value<RingsDetectionValue> other) {
        throw new NotSupportedException();
    }

    @Override
    public int compareTo(RingsDetectionValue o) {
        throw new NotSupportedException();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.path.read(in);
        this.inEdgeProp.read(in);
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        this.path.write(out);
        this.inEdgeProp.write(out);
    }

    @Override
    public RingsDetectionValue copy() {
        throw new NotSupportedException();
    }

    public IdList path() {
        return this.path;
    }

    public void addPath(Vertex vertex) {
        this.path.add(vertex.id());
    }

    public Properties degreeEdgeProp() {
        return this.inEdgeProp;
    }

    public void degreeEdgeProp(Properties properties) {
        this.inEdgeProp = properties;
    }

    @Override
    public Object object() {
        throw new NotSupportedException();
    }
}
