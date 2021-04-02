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

package com.baidu.hugegraph.computer.core.network.netty;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.computer.core.network.TransportHandler;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.network.session.ClientSession;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;

public class NettyTransportClient implements TransportClient {

    private static final Logger LOG = Log.logger(NettyTransportClient.class);

    private final Channel channel;
    private final ConnectionID connectionID;
    private final NettyClientFactory clientFactory;
    private final TransportHandler handler;
    private final ClientSession clientSession;

    protected NettyTransportClient(Channel channel, ConnectionID connectionID,
                                   NettyClientFactory clientFactory,
                                   TransportHandler clientHandler) {
        E.checkArgumentNotNull(clientHandler,
                               "The handler param can't be null");
        this.initChannel(channel, clientFactory.protocol(), clientHandler);
        this.channel = channel;
        this.connectionID = connectionID;
        this.clientFactory = clientFactory;
        this.handler = clientHandler;
        this.clientSession = new ClientSession();
    }

    public Channel channel() {
        return this.channel;
    }

    @Override
    public ConnectionID connectionID() {
        return this.connectionID;
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return (InetSocketAddress) this.channel.remoteAddress();
    }

    @Override
    public boolean active() {
        return this.channel.isActive();
    }

    @Override
    public synchronized void startSession() throws IOException {
        // TODO: startSession
    }

    @Override
    public synchronized void send(MessageType messageType, int partition,
                                  ByteBuffer buffer) throws IOException {
        // TODO: send message
    }

    @Override
    public synchronized void finishSession() throws IOException {
        // TODO: finishSession
    }

    @Override
    public void close() {
        if (this.channel != null) {
            long timeout = this.clientFactory.conf().closeTimeout();
            this.channel.close().awaitUninterruptibly(timeout, MILLISECONDS);
        }
    }

    public ClientSession clientSession() {
        return this.clientSession;
    }

    public TransportHandler handler() {
        return this.handler;
    }

    private void initChannel(Channel channel, NettyProtocol protocol,
                             TransportHandler handler) {
        protocol.replaceClientHandler(channel, this);
        // Client ready notice
        handler.channelActive(this.connectionID);
    }
}
