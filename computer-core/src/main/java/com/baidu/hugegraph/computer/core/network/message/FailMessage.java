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

import static com.baidu.hugegraph.computer.core.network.TransportUtil.encodeString;

import java.nio.ByteBuffer;

import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.buffer.NioManagedBuffer;

import io.netty.buffer.ByteBuf;

public class FailMessage extends AbstractMessage implements ResponseMessage {

    private final int failAckId;
    private final int errorCode;
    private final String msg;

    public FailMessage(int failAckId, int errorCode, String msg) {
        this.failAckId = failAckId;
        this.errorCode = errorCode;
        this.msg = msg;
    }

    @Override
    public MessageType type() {
        return MessageType.FAIL;
    }

    @Override
    public int ackId() {
        return this.failAckId;
    }

    @Override
    protected ManagedBuffer encodeBody(ByteBuf buf) {
        byte[] bytes = encodeString(this.msg);
        int bodyLength = 4 + bytes.length;
        // Copy to direct memory
        ByteBuffer buffer = ByteBuffer.allocateDirect(bodyLength)
                                      .putInt(this.errorCode)
                                      .put(bytes);
        // Flip to make it readable
        buffer.flip();
        return new NioManagedBuffer(buffer);
    }

    public static FailMessage parseFrom(ByteBuf buf) {
        int failAckId = buf.readInt();
        int failCode = 0;
        String failMsg = null;
        // Skip partition
        buf.skipBytes(4);
        // Skip body-length
        int bodyLength = buf.readInt();

        if (bodyLength >= 4) {
            failCode = buf.readInt();
            failMsg = TransportUtil.readString(buf);
        }
        return new FailMessage(failAckId, failCode, failMsg);
    }

    public String msg() {
        return this.msg;
    }

    public int errorCode() {
        return this.errorCode;
    }
}
