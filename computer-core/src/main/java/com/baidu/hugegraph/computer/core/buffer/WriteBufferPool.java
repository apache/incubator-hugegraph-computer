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

package com.baidu.hugegraph.computer.core.buffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.util.Log;

public class WriteBufferPool {

    private static final Logger LOG = Log.logger(WriteBufferPool.class);

    private final Config config;
    private final Map<Integer, WriteBuffer> buffers;

    public WriteBufferPool(ComputerContext context) {
        this.config = context.config();
        this.buffers = new ConcurrentHashMap<>();
    }

    public WriteBuffer getOrCreateBuffer(int partitionId) {
        WriteBuffer buffer = this.buffers.get(partitionId);
        if (buffer == null) {
            int size = this.config.get(ComputerOptions.WRITE_BUFFER_SIZE);
            int capacity = this.config.get(
                           ComputerOptions.WRITE_BUFFER_CAPACITY);
            // Maybe create an extra buffer object, but it's harmless
            WriteBuffer newBuffer = new WriteBuffer(size, capacity);
            buffer = this.buffers.putIfAbsent(partitionId, newBuffer);
            if (buffer == null) {
                buffer = newBuffer;
                LOG.info("Create a WriteBufferPool for partition {}",
                         partitionId);
            }
        }
        return buffer;
    }
}
