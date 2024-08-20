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

package org.apache.hugegraph.computer.core.sender;

import java.util.concurrent.CompletableFuture;

import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.message.MessageType;

public interface MessageSender {

    /**
     * Send control message, like START and FINISH
     * @param workerId target workerId
     * @param type message type
     */
    CompletableFuture<Void> send(int workerId, MessageType type)
                                 throws InterruptedException;

    /**
     * Send data message, like VERTEX, EDGE or MSG
     * @param workerId target workerId
     * @param message message payload
     */
    void send(int workerId, QueuedMessage message) throws InterruptedException;

    /**
     * Invoked when the channel associated with the given connectionId has
     * an exception is thrown processing message.
     */
    void transportExceptionCaught(TransportException cause, ConnectionId connectionId);
}
