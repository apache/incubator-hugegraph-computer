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
import com.baidu.hugegraph.computer.core.network.buffer.NioManagedBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

public class FailMessage extends AbstractMessage implements ResponseMessage {

    public static int DEFAULT_FAIL_CODE = 1;

    private final int failAckId;
    private final int failCode;
    private final String failMsg;

    @Override
    protected ManagedBuffer encodeBody(ByteBuf buf) {
        byte[] bytes = encodeString(this.failMsg);
        // Copy to direct memory
        ByteBuffer buffer = ByteBuffer.allocateDirect(4 + bytes.length)
                                      .putInt(this.failCode)
                                      .put(bytes);
        // Flip to make it readable
        buffer.flip();
        return new NioManagedBuffer(buffer);
    }

    public FailMessage(int failAckId, int failCode, String failMsg) {
        this.failAckId = failAckId;
        this.failCode = failCode;
        this.failMsg = failMsg;
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

        int failCode = buf.readInt();
        byte[] bytes = ByteBufUtil.getBytes(buf);
        String failMsg = null;
        if (bytes != null) {
            failMsg = decodeString(bytes);
        }
        return new FailMessage(failAckId, failCode, failMsg);
    }

    public String failMessage() {
        return this.failMsg;
    }

    public int failCode() {
        return this.failCode;
    }
}
