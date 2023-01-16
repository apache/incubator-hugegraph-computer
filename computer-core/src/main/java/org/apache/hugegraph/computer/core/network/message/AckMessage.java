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

import io.netty.buffer.ByteBuf;

public class AckMessage extends AbstractMessage implements ResponseMessage {

    public AckMessage(int ackId) {
        super(ackId);
    }

    public AckMessage(int ackId, int partition) {
        super(ackId, partition);
    }

    @Override
    public MessageType type() {
        return MessageType.ACK;
    }

    public static AckMessage parseFrom(ByteBuf buf) {
        int ackId = buf.readInt();
        int partition = buf.readInt();
        // Skip body-length
        buf.skipBytes(Integer.BYTES);
        return new AckMessage(ackId, partition);
    }
}
