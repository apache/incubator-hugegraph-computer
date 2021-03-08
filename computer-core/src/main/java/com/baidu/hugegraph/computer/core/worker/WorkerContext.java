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

package com.baidu.hugegraph.computer.core.worker;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

/**
 * This class is the interface used by computation.
 */
public interface WorkerContext {

    /**
     * Get config
     */
    Config config();

    <A extends Value> void aggregate(String name, A value);

    <A extends Value> A aggregatedValue(String name);

    /**
     * Send value to specified target vertex.
     */
    void sendMessage(Id target, Value value);

    /**
     * Send value to all edges of vertex.
     */
    void sendMessageToAllEdges(Vertex vertex, Value value);

    /**
     * @return the total vertex count of the graph. The value may vary from
     * superstep to superstep.
     */
    long totalVertexCount();

    /**
     * @return the total edge count of the graph. The value may vary from
     * superstep to superstep.
     */
    long totalEdgeCount();

    /**
     * @return the current superstep.
     */
    int superstep();
}
