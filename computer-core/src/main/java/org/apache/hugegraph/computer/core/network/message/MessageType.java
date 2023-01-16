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

import static org.apache.hugegraph.computer.core.network.message.MessageType.Category.CONTROL;
import static org.apache.hugegraph.computer.core.network.message.MessageType.Category.DATA;

import org.apache.hugegraph.computer.core.common.SerialEnum;

import io.netty.buffer.ByteBuf;

public enum MessageType implements SerialEnum {

    MSG(0x01, DATA),
    VERTEX(0x02, DATA),
    EDGE(0x03, DATA),
    ACK(0x80, CONTROL),
    FAIL(0X81, CONTROL),
    START(0x82, CONTROL),
    FINISH(0x83, CONTROL),
    PING(0x84, CONTROL),
    PONG(0x85, CONTROL),
    PAUSE_RECV(0x86, CONTROL),
    RESUME_RECV(0x87, CONTROL);

    static {
        SerialEnum.register(MessageType.class);
    }

    private final byte code;
    private final Category category;

    MessageType(int code, Category category) {
        assert code >= 0 && code <= 255;
        this.code = (byte) code;
        this.category = category;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public Category category() {
        return this.category;
    }

    public static MessageType decode(ByteBuf buf) {
        byte code = buf.readByte();
        return SerialEnum.fromCode(MessageType.class, code);
    }

    public enum Category {
        DATA,
        CONTROL
    }
}
