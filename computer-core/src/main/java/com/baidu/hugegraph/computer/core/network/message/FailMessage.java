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

import static com.baidu.hugegraph.computer.core.network.TransportUtil.decodeString;
import static com.baidu.hugegraph.computer.core.network.TransportUtil.encodeString;

import java.nio.ByteBuffer;

import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.buffer.NettyManagedBuffer;
import com.baidu.hugegraph.computer.core.network.buffer.NioManagedBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class FailMessage extends AbstractMessage implements ResponseMessage {

    private final int failAckId;
    private final String failMsg;

    public static FailMessage createFailMessage(int failAckId,
                                                String failMsg) {
        byte[] bytes = encodeString(failMsg);
        // Copy to direct memory
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length).put(bytes);
        // Flip to make it readable
        buffer.flip();
        return new FailMessage(failAckId, new NioManagedBuffer(buffer),
                               failMsg);
    }

    public FailMessage(int failAckId, ManagedBuffer failBuffer) {
        this(failAckId, failBuffer, null);
    }

    public FailMessage(int failAckId, ManagedBuffer failBuffer,
                       String failMsg) {
        super(failBuffer);
        this.failAckId = failAckId;
        if (failMsg != null) {
            this.failMsg = failMsg;
        } else {
            byte[] bytes = ByteBufUtil.getBytes(failBuffer.nettyByteBuf());
            this.failMsg = decodeString(bytes);
        }
    }

    @Override
    public MessageType type() {
        return MessageType.FAIL;
    }

    @Override
    public int ackId() {
        return this.failAckId;
    }

    public static FailMessage parseFrom(ByteBuf buf) {
        int failAckId = buf.readInt();
        // Skip partition
        buf.skipBytes(4);
        // Skip body-length
        buf.skipBytes(4);
        ManagedBuffer managedBuffer = new NettyManagedBuffer(buf.duplicate());
        managedBuffer.retain();
        return new FailMessage(failAckId, managedBuffer);
    }

    public String failMsg() {
        return this.failMsg;
    }
}
