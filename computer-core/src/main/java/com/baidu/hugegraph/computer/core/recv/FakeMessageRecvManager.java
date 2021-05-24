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

package com.baidu.hugegraph.computer.core.recv;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.util.Log;

public class FakeMessageRecvManager implements Manager, MessageHandler {

    private static final Logger LOG = Log.logger(FakeMessageRecvManager.class);

    public static final String NAME = "message_recv";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        LOG.info("MessageRecvManager initialized");
    }

    @Override
    public void beforeSuperstep(Config config, int superstep) {
        LOG.info("Reinitialized message recv partitions");
    }

    @Override
    public void afterSuperstep(Config config, int superstep) {
        LOG.info("Flush all buffers and wait sorted");
    }

    @Override
    public void close(Config config) {
        LOG.info("MessageRecvManager closed");
    }

    @Override
    public void handle(MessageType messageType, int partition,
                       ManagedBuffer buffer) {
        switch (messageType) {
            case START:
            case VERTEX:
            case EDGE:
            case MSG:
            case FINISH:
                LOG.info("Receive message {}", messageType);
                break;
            default:
                throw new ComputerException(
                          "Unable handle ManagedBuffer with type '%s'",
                          messageType.name());
        }
    }

    @Override
    public void channelActive(ConnectionId connectionId) {

    }

    @Override
    public void channelInactive(ConnectionId connectionId) {

    }

    @Override
    public void exceptionCaught(TransportException cause,
                                ConnectionId connectionId) {
        LOG.warn("Exception caught for connection:{}, root cause:{}",
                 connectionId, cause);
    }
}
