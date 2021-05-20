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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.util.Log;

public class MessageSendPartitions {

    private static final Logger LOG = Log.logger(MessageSendPartitions.class);

    // Each partition has a MessageSendPartition object
    private final MessageSendPartition[] partitions;

    public MessageSendPartitions(ComputerContext context) {
        Config config = context.config();
        int partitionCount = config.get(ComputerOptions.JOB_PARTITIONS_COUNT);
        this.partitions = new MessageSendPartition[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            /*
             * It depends on the concrete implementation of the
             * partition algorithm, which is not elegant.
             */
            this.partitions[i] = new MessageSendPartition(context);
        }
    }

    public MessageSendPartition get(int partitionId) {
        if (partitionId < 0 || partitionId >= this.partitions.length)  {
            throw new ComputerException("Invalid partitionId %s", partitionId);
        }
        return this.partitions[partitionId];
    }

    public Map<Integer, MessageSendPartition> all() {
        Map<Integer, MessageSendPartition> all = new LinkedHashMap<>();
        for (int partitionId = 0; partitionId < this.partitions.length;
             partitionId++) {
            all.put(partitionId, this.partitions[partitionId]);
        }
        return all;
    }
}
