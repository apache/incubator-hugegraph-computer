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

package com.baidu.hugegraph.computer.core.store;

import com.baidu.hugegraph.computer.core.config.Config;

public interface GraphSorter {

    /**
     * Init the sorter.
     */
    void init(Config config);

    /**
     * Sort vertex buffer by increasing order of vertex id.
     * There three part for a vertex, vertex id, edges, properties.
     * Merge edges and properties if a vertex exists multiple times.
     * @return The new buffer sorted.
     */
    HgkvInput sortVertexBuffer(HgkvInput input);

    /**
     * Sort Message buffer by increasing order of vertex id.
     * The format in input buffer is one vertex id followed by the message
     * sent to the vertex id. The vertex id may exists many times. For
     * example | vertex id 1 | message for vertex id 1 | vertex id 2 | message
     * for vertex id 2 | vertex id 1 | message for vertex id 1 | and so on.
     * If the combiner for message is set. There is only one message for
     * a vertex after sort. The format of output buffer is
     * | vertex id 1 | message for vertex id 1 | vertex id 2 | message 2 for
     * vertex id 2 | and so on.
     * If the combiner for message is not set. There are several messages for
     * aa vertex after sort. The format of output buffer is | vertex id 1 |
     * message bytes count for for vertex id1 | message count for vertex id 1 |
     * a sequence of message for vertex id 1 | and so on.
     * @return The new buffer sorted.
     */
    HgkvInput sortMessageBuffer(HgkvInput input);

    /**
     * Merge the vertex into a file, combine edges and properties.
     * The format of input buffer is output of {@link #sortVertexBuffer}.
     * @return the FileInfo after sort.
     */
    HgkvFile sortVertexBuffer(BufferInfoList buffers);

    /**
     * Merge the messages in buffers into a file, combine edges and properties.
     * The format of input buffer is output of {@link #sortMessageBuffer}.
     * @return the FileInfo after sort.
     */
    HgkvFile sortMessageBuffer(BufferInfoList buffers);
}
