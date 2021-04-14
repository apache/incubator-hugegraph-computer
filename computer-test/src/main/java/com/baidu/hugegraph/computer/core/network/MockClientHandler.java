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

package com.baidu.hugegraph.computer.core.network;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.util.Log;

public class MockClientHandler implements ClientHandler {

    private static final Logger LOG = Log.logger(MockClientHandler.class);

    @Override
    public void channelActive(ConnectionId connectionId) {
        LOG.info("Client connection active, connectionId: {}", connectionId);
    }

    @Override
    public void channelInactive(ConnectionId connectionId) {
        LOG.info("Client connection inActive, connectionId: {}", connectionId);
    }

    @Override
    public void exceptionCaught(TransportException cause,
                                ConnectionId connectionId) {
        // Close the client associated with the given connectionId
        LOG.error("Client connection exception, connectionId: {}, cause: ",
                  connectionId, cause);
    }

    @Override
    public void sendAvailable(ConnectionId connectionId) {
        LOG.info("Client is able to send data, connectionId: {}", connectionId);
    }
}
