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

import com.baidu.hugegraph.computer.core.network.ClientHandler;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.network.session.ClientSession;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

public class NettyTransportClient implements TransportClient {

    private static final Logger LOG = Log.logger(NettyTransportClient.class);

    private final Channel channel;
    private final ConnectionId connectionId;
    private final NettyClientFactory clientFactory;
    private final ClientHandler handler;
    private final ClientSession clientSession;
    private final ClientChannelListenerOnWrite listenerOnWrite;

    protected NettyTransportClient(Channel channel, ConnectionId connectionId,
                                   NettyClientFactory clientFactory,
                                   ClientHandler clientHandler) {
        E.checkArgumentNotNull(clientHandler,
                               "The handler param can't be null");
        this.initChannel(channel, connectionId, clientFactory.protocol(),
                         clientHandler);
        this.channel = channel;
        this.connectionId = connectionId;
        this.clientFactory = clientFactory;
        this.handler = clientHandler;
        this.clientSession = new ClientSession();
        this.listenerOnWrite = new ClientChannelListenerOnWrite();
    }

    public Channel channel() {
        return this.channel;
    }

    @Override
    public ConnectionId connectionId() {
        return this.connectionId;
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
    public synchronized boolean send(MessageType messageType, int partition,
                                     ByteBuffer buffer) throws IOException {
        return false;
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

    public ClientHandler handler() {
        return this.handler;
    }

    protected boolean checkSendAvailable() {
        // TODO: checkSendAvailable
        return this.channel.isWritable();
    }

    private void initChannel(Channel channel, ConnectionId connectionId,
                             NettyProtocol protocol, ClientHandler handler) {
        protocol.replaceClientHandler(channel, this);
        // Client ready notice
        handler.channelActive(connectionId);
    }

    private class ClientChannelListenerOnWrite
            extends ChannelFutureListenerOnWrite {

        ClientChannelListenerOnWrite() {
            super(NettyTransportClient.this.handler);
        }

        @Override
        public void onSuccess(Channel channel, ChannelFuture future) {
            super.onSuccess(channel, future);
            NettyTransportClient client = NettyTransportClient.this;
            if (client.checkSendAvailable()) {
                client.handler.sendAvailable(client.connectionId);
            }
        }
    }
}
