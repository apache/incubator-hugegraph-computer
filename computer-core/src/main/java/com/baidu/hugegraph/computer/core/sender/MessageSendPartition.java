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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.receiver.MessageStat;

public class MessageSendPartition {

    private final ComputerContext context;
    private final int threshold;
    private final int capacity;

    private final Map<Thread, WriteBuffers> buffers;

    public MessageSendPartition(ComputerContext context,
                                int threshold, int capacity) {
        this.context = context;
        this.threshold = threshold;
        this.capacity = capacity;

        this.buffers = new ConcurrentHashMap<>();
    }

    public WriteBuffers get() {
        Thread current = Thread.currentThread();
        WriteBuffers buffer = this.buffers.get(current);
        if (buffer == null) {
            buffer = new WriteBuffers(this.context, this.threshold,
                                      this.capacity);
            this.buffers.put(current, buffer);
        }
        return buffer;
    }

    public void clear() {
        this.buffers.clear();
    }

    public void resetMessageWritten() {
        for (WriteBuffers buffer : this.buffers.values()) {
            buffer.resetMessageWritten();
        }
    }

    public MessageStat messageWritten() {
        MessageStat partitionStat = new MessageStat();
        for (WriteBuffers buffer : this.buffers.values()) {
            partitionStat.increase(buffer.messageWritten());
        }
        return partitionStat;
    }

    public Collection<WriteBuffers> buffers() {
        return this.buffers.values();
    }
}
