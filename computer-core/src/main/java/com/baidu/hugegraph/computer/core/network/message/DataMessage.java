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

package com.baidu.hugegraph.computer.core.network.message;

import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.buffer.NettyManagedBuffer;
import com.baidu.hugegraph.util.E;

import io.netty.buffer.ByteBuf;

public class DataMessage extends AbstractMessage implements RequestMessage {

    public final int requestId;
    private final MessageType type;

    public DataMessage(MessageType type, int requestId,
                       int partition, ManagedBuffer data) {
        super(partition, data);
        E.checkArgument(requestId > 0,
                        "The data requestId must be > 0, but got %s",
                        requestId);
        this.requestId = requestId;
        this.type = type;
    }

    @Override
    public MessageType type() {
        return this.type;
    }

    @Override
    public int requestId() {
        return this.requestId;
    }

    /**
     * Decoding uses the given ByteBuf as our data, and will zero-copy it.
     */
    public static DataMessage parseFrom(MessageType type, ByteBuf buf) {
        int requestId = buf.readInt();
        int partition = buf.readInt();

        int bodyLength = buf.readInt();
        // Slice body and retain it
        ByteBuf byteBuf = buf.readRetainedSlice(bodyLength);
        buf.skipBytes(bodyLength);
        ManagedBuffer managedBuffer = new NettyManagedBuffer(byteBuf);
        return new DataMessage(type, requestId, partition, managedBuffer);
    }
}
