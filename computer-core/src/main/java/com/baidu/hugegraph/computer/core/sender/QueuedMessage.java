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

import java.nio.ByteBuffer;

import com.baidu.hugegraph.computer.core.network.message.MessageType;

public class QueuedMessage {

    private final int partitionId;
    private final int workerId;
    private final MessageType type;
    private final ByteBuffer buffer;

    public QueuedMessage(int partitionId, int workerId,
                         MessageType type, ByteBuffer buffer) {
        this.partitionId = partitionId;
        this.workerId = workerId;
        this.type = type;
        this.buffer = buffer;
    }

    public int partitionId() {
        return this.partitionId;
    }

    public int workerId() {
        return this.workerId;
    }

    public MessageType type() {
        return this.type;
    }

    public ByteBuffer buffer() {
        return this.buffer;
    }

    @Override
    public String toString() {
        return String.format("QueuedMessage{partitionId=%s, workerId=%s, " +
                             "type=%s}",
                             this.partitionId, this.workerId, this.type);
    }
}
