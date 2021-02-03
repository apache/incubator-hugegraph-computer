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

package com.baidu.hugegraph.computer.core.allocator;

import java.util.function.Supplier;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

import io.netty.util.Recycler;

public final class Allocator {

    private final GraphFactory factory;
    private final Recycler<RecyclerReference<Vertex>> vertexRecycler;
    private final Recycler<RecyclerReference<Edge>> edgeRecycler;

    public Allocator(Config config) {
        this.factory = new GraphFactory();

        int capacityPerThread =
        config.get(ComputerOptions.ALLOCATOR_MAX_VERTICES_PER_THREAD);
        this.vertexRecycler = this.newRecycler(capacityPerThread,
                                               factory::createVertex);
        this.edgeRecycler = this.newRecycler(capacityPerThread,
                                             factory::createEdge);
    }

    private <T extends Recyclable> Recycler<RecyclerReference<T>>
                                   newRecycler(int capacityPerThread,
                                               Supplier<T> supplier) {
        // TODO: Add more params for Recycler
        return new Recycler<RecyclerReference<T>>(capacityPerThread) {
            @Override
            protected RecyclerReference<T> newObject(
                      Recycler.Handle<RecyclerReference<T>> handle) {
                T recyclable = supplier.get();
                return new RecyclerReference<T>(recyclable, handle);
            }
        };
    }

    public RecyclerReference<Vertex> newVertex() {
        return this.vertexRecycler.get();
    }

    public void freeVertex(RecyclerReference<Vertex> reference) {
        reference.close();
    }

    public RecyclerReference<Edge> newEdge() {
        return this.edgeRecycler.get();
    }

    public void freeEdge(RecyclerReference<Edge> reference) {
        reference.close();
    }
}
