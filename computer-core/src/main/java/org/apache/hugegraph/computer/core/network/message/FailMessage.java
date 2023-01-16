/*
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

package org.apache.hugegraph.computer.core.network.message;

import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.util.E;

import io.netty.buffer.ByteBuf;

@Deprecated
public class FailMessage extends AbstractMessage implements ResponseMessage {

    private final int errorCode;
    private final String message;

    public FailMessage(int ackId, int errorCode, String message) {
        super(ackId);
        E.checkNotNull(message, "message");
        this.errorCode = errorCode;
        this.message = message;
    }

    @Override
    public MessageType type() {
        return MessageType.FAIL;
    }

    @Override
    protected NetworkBuffer encodeBody(ByteBuf buf) {
        buf.writeInt(this.errorCode);
        TransportUtil.writeString(buf, this.message);
        return null;
    }

    public static FailMessage parseFrom(ByteBuf buf) {
        int failAckId = buf.readInt();
        int failCode = 0;
        String failMsg = null;
        // Skip partition
        buf.skipBytes(Integer.BYTES);

        // Decode body buffer
        int bodyLength = buf.readInt();
        if (bodyLength >= Integer.BYTES) {
            failCode = buf.readInt();
            failMsg = TransportUtil.readString(buf);
        }

        return new FailMessage(failAckId, failCode, failMsg);
    }

    public static int remainingBytes(ByteBuf buf) {
        buf.markReaderIndex();
        // Skip ackId
        buf.skipBytes(Integer.BYTES);
        // Skip partition
        buf.skipBytes(Integer.BYTES);
        int bodyLength = buf.readInt();
        buf.resetReaderIndex();
        return bodyLength - buf.readableBytes();
    }

    public String message() {
        return this.message;
    }

    public int errorCode() {
        return this.errorCode;
    }
}
