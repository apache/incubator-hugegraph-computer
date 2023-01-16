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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.TransportClient;
import org.apache.hugegraph.computer.core.network.message.MessageType;

public class MockTransportClient implements TransportClient {

    private ConnectionId connectionId;

    public MockTransportClient() {
        InetSocketAddress address = new InetSocketAddress("localhost", 8080);
        this.connectionId = new ConnectionId(address);
    }

    @Override
    public void startSession() throws TransportException {
        throw new ComputerException("Not implemented");
    }

    @Override
    public CompletableFuture<Void> startSessionAsync()
                                   throws TransportException {
        throw new ComputerException("Not implemented");
    }

    @Override
    public boolean send(MessageType messageType,
                        int partition,
                        ByteBuffer buffer) throws TransportException {
        throw new ComputerException("Not implemented");
    }

    @Override
    public void finishSession() throws TransportException {
        throw new ComputerException("Not implemented");
    }

    @Override
    public CompletableFuture<Void> finishSessionAsync()
                                   throws TransportException {
        throw new ComputerException("Not implemented");
    }

    @Override
    public ConnectionId connectionId() {
        return this.connectionId;
    }

    @Override
    public InetSocketAddress remoteAddress() {
        throw new ComputerException("Not implemented");
    }

    @Override
    public boolean active() {
        throw new ComputerException("Not implemented");
    }

    @Override
    public boolean sessionActive() {
        throw new ComputerException("Not implemented");
    }

    @Override
    public void close() {
        throw new ComputerException("Not implemented");
    }
}
