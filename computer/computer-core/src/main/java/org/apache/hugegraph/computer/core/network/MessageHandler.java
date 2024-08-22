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

package org.apache.hugegraph.computer.core.network;

import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.network.message.MessageType;

public interface MessageHandler extends TransportHandler {

    /**
     * Handle the buffer received. There are two buffer list for a partition,
     * one for sorting and one for receiving new buffers. It may block the
     * caller if the receiving list reached threshold and the sorting list is
     * sorting in process.
     */
    void handle(MessageType messageType, int partition, NetworkBuffer buffer);

    /**
     * Build a output path.
     */
    String genOutputPath(MessageType messageType, int partition);

    /**
     * Notify start-session completed on server-side.
     */
    void onStarted(ConnectionId connectionId);

    /**
     * Notify finish-session completed on server-side.
     */
    void onFinished(ConnectionId connectionId);
}
