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

import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.buffer.NettyManagedBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class FailMessage extends AbstractMessage implements ResponseMessage {

    private final int failAckId;
    private final String failString;

    public static FailMessage createFailMessage(int failAckId,
                                                String failString) {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(encodeString(failString));
        return new FailMessage(failAckId, new NettyManagedBuffer(byteBuf),
                               failString);
    }

    public FailMessage(int failAckId, ManagedBuffer failBuffer) {
        this(failAckId, failBuffer, null);
    }

    public FailMessage(int failAckId, ManagedBuffer failBuffer,
                       String failString) {
        super(failAckId, failBuffer);
        this.failAckId = failAckId;
        if (failString != null) {
            this.failString = failString;
        } else {
            ByteBuf byteBuf = failBuffer.nettyByteBuf();
            this.failString = decodeString(byteBuf.array());
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
        buf.skipBytes(4);
        buf.retain();
        return new FailMessage(failAckId,
                               new NettyManagedBuffer(buf.duplicate()));
    }

    public String failMessage() {
        return this.failString;
    }
}
