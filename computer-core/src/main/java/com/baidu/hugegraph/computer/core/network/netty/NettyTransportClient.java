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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.MessageType;
import com.baidu.hugegraph.computer.core.network.Transport4Client;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.util.Log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class NettyTransportClient implements Transport4Client {

    private static final Logger LOG = Log.logger(NettyTransportClient.class);

    private volatile Channel channel;
    private final ConnectionID connectionID;
    private final NettyClientFactory clientFactory;

    protected NettyTransportClient(Channel channel, ConnectionID connectionID,
                                   NettyClientFactory clientFactory) {
        this.channel = channel;
        this.connectionID = connectionID;
        this.clientFactory = clientFactory;
    }

    public synchronized void connect() throws IOException {
        InetSocketAddress address = this.connectionID.socketAddress();
        TransportConf conf = this.clientFactory.conf();
        int retries = conf.networkRetries();
        this.channel = this.clientFactory.doConnectWithRetries(address,
                                                               retries);
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
    public boolean isActive() {
        return (this.channel.isOpen() || this.channel.isActive());
    }

    @Override
    public void startSession() throws IOException {
        // TODO startSession
    }

    @Override
    public void send(MessageType messageType, int partition,
                     ByteBuf buffer) throws IOException {
        // TODO send message
    }

    @Override
    public void finishSession() throws IOException {
        // TODO finishSession
    }

    @Override
    public void close() {
        if (this.channel != null) {
            long timeout = this.clientFactory.conf().closeTimeout();
            this.channel.close().awaitUninterruptibly(timeout, MILLISECONDS);
        }
    }
}
