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

import org.apache.commons.lang3.NotImplementedException;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdFactory;
import com.baidu.hugegraph.computer.core.graph.value.ValueFactory;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public class StreamGraphInput implements GraphComputeInput {

    private final RandomAccessInput in;
    private final Config config;
    private final GraphFactory graphFactory;
    private final ValueFactory valueFactory;

    public StreamGraphInput(ComputerContext context, RandomAccessInput in) {
        this.config = context.config();
        this.graphFactory = context.graphFactory();
        this.valueFactory = context.valueFactory();
        this.in = in;
    }

    @Override
    public Vertex readVertex() throws IOException {
        // When data receiver merged, implement it
        throw new NotImplementedException("StreamGraphInput.readVertex()");
    }

    @Override
    public Id readId() throws IOException {
        byte code = this.in.readByte();
        Id id = IdFactory.createId(code);
        id.read(this.in);
        return id;
    }
}
