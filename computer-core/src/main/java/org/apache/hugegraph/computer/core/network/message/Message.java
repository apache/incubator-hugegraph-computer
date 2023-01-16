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

import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;

import io.netty.buffer.ByteBuf;

public interface Message {

    /**
     * Serializes this object by writing into the given ByteBuf.
     *
     * @param buf {@link ByteBuf} the header buffer, if use zero-copy.
     * Otherwise it will contain header and body.
     * @return {@link NetworkBuffer} body buffer, if use zero-copy
     */
    NetworkBuffer encode(ByteBuf buf);

    /**
     * Used to identify this message type.
     */
    MessageType type();

    /**
     * Whether to include the body of the message
     * in the same frame as the message.
     */
    boolean hasBody();

    /**
     * An optional body for the message.
     */
    NetworkBuffer body();

    /**
     * The message sequence number
     */
    int sequenceNumber();

    /**
     * The partition id
     */
    int partition();

    /**
     * Release the message
     */
    void release();
}
