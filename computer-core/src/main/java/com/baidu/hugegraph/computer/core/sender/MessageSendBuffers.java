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

import java.util.LinkedHashMap;
import java.util.Map;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;

public class MessageSendBuffers {

    /*
     * Currently there is only one WriteBuffer object in each partition.
     * Add a MessageSendPartition class when find that we really need it
     * to encapsulate more objects.
     */
    private final WriteBuffers[] buffers;

    public MessageSendBuffers(ComputerContext context) {
        Config config = context.config();
        int partitionCount = config.get(ComputerOptions.JOB_PARTITIONS_COUNT);
        int threshold = config.get(
                        ComputerOptions.WORKER_WRITE_BUFFER_THRESHOLD);
        int capacity = config.get(
                       ComputerOptions.WORKER_WRITE_BUFFER_INIT_CAPACITY);
        this.buffers = new WriteBuffers[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            /*
             * It depends on the concrete implementation of the
             * partition algorithm, which is not elegant.
             */
            this.buffers[i] = new WriteBuffers(context, threshold, capacity);
        }
    }

    public WriteBuffers get(int partitionId) {
        if (partitionId < 0 || partitionId >= this.buffers.length)  {
            throw new ComputerException("Invalid partitionId %s", partitionId);
        }
        return this.buffers[partitionId];
    }

    public Map<Integer, WriteBuffers> all() {
        Map<Integer, WriteBuffers> all = new LinkedHashMap<>();
        for (int i = 0; i < this.buffers.length; i++) {
            all.put(i, this.buffers[i]);
        }
        return all;
    }
}
