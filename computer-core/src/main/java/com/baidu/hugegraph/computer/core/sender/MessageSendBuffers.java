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

package com.baidu.hugegraph.computer.core.sender;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;

public class MessageSendBuffers {

    /*
     * Add a MessageSendPartition class when find that we really need it
     * to encapsulate more objects. currently, there is only WriteBuffer
     */
    private final Map<Integer, WriteBuffers> buffers;

    public MessageSendBuffers(ComputerContext context) {
        Config config = context.config();
        int partitionCount = config.get(ComputerOptions.JOB_PARTITIONS_COUNT);
        int threshold = config.get(ComputerOptions.WRITE_BUFFER_THRESHOLD);
        int capacity = config.get(ComputerOptions.WRITE_BUFFER_CAPACITY);
        this.buffers = new HashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            /*
             * It depends on the concrete implementation of the
             * partition algorithm, which is not elegant.
             */
            int partitionId = i;
            WriteBuffers buffer = new WriteBuffers(threshold, capacity);
            this.buffers.put(partitionId, buffer);
        }
    }

    public WriteBuffers get(int partitionId) {
        WriteBuffers buffer = this.buffers.get(partitionId);
        if (buffer == null)  {
            throw new ComputerException("Invalid partitionId %s", partitionId);
        }
        return buffer;
    }

    public Map<Integer, WriteBuffers> all() {
        return Collections.unmodifiableMap(this.buffers);
    }
}
