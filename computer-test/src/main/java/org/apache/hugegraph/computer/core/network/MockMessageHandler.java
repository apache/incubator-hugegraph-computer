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

import java.io.File;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.network.buffer.FileRegionBuffer;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class MockMessageHandler implements MessageHandler {

    private static final Logger LOG = Log.logger(MockMessageHandler.class);

    @Override
    public void handle(MessageType messageType, int partition,
                       NetworkBuffer buffer) {
        LOG.info("Receive data from remote, messageType: {}, partition: {}, " +
                 "buffer readable length: {}", messageType.name(), partition,
                 buffer != null ? buffer.length() : null);

        if (buffer != null) {
            if (buffer instanceof FileRegionBuffer) {
                String path = ((FileRegionBuffer) buffer).path();
                LOG.info("path: {}", path);
                FileUtils.deleteQuietly(new File(path));
            } else {
                buffer.copyToByteArray();
            }
        }
    }

    @Override
    public String genOutputPath(MessageType messageType, int partition) {
        return "./" + UUID.randomUUID().toString();
    }

    @Override
    public void onStarted(ConnectionId connectionId) {
        LOG.info("Start session completed, connectionId: {}",
                 connectionId);
    }

    @Override
    public void onFinished(ConnectionId connectionId) {
        LOG.info("Finish session completed, connectionId: {}",
                 connectionId);
    }

    @Override
    public void onChannelActive(ConnectionId connectionId) {
        LOG.info("Server channel active, connectionId: {}", connectionId);
    }

    @Override
    public void onChannelInactive(ConnectionId connectionId) {
        LOG.info("Server channel inActive, connectionId: {}", connectionId);
    }

    @Override
    public void exceptionCaught(TransportException cause,
                                ConnectionId connectionId) {
        LOG.info("Server channel exception, connectionId: {}, cause:",
                 connectionId, cause);
    }
}
