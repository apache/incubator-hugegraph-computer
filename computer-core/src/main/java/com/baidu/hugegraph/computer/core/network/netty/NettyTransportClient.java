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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ClientHandler;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.network.session.ClientSession;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

public class NettyTransportClient implements TransportClient {

    private static final Logger LOG = Log.logger(NettyTransportClient.class);

    private final Channel channel;
    private final ConnectionId connectionId;
    private final NettyClientFactory clientFactory;
    private final ClientHandler handler;
    private final ClientSession session;
    private final long syncRequestTimeout;
    private final long finishSessionTimeout;

    protected NettyTransportClient(Channel channel, ConnectionId connectionId,
                                   NettyClientFactory clientFactory,
                                   ClientHandler clientHandler) {
        E.checkArgumentNotNull(clientHandler,
                               "The clientHandler param can't be null");
        this.initChannel(channel, connectionId, clientFactory.protocol(),
                         clientHandler);
        this.channel = channel;
        this.connectionId = connectionId;
        this.clientFactory = clientFactory;
        this.handler = clientHandler;

        TransportConf conf = this.clientFactory.conf();
        this.syncRequestTimeout = conf.syncRequestTimeout();
        this.finishSessionTimeout = conf.finishSessionTimeout();
        this.session = new ClientSession(conf, this.createSendFunction());
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

    protected ClientSession clientSession() {
        return this.session;
    }

    public ClientHandler clientHandler() {
        return this.handler;
    }

    private void initChannel(Channel channel, ConnectionId connectionId,
                             NettyProtocol protocol, ClientHandler handler) {
        protocol.replaceClientHandler(channel, this);
        // Client stateReady notice
        handler.channelActive(connectionId);
    }

    private Function<Message, Future<Void>> createSendFunction() {
        ChannelFutureListener listener = new ClientChannelListenerOnWrite();
        return message -> {
            ChannelFuture channelFuture = this.channel.writeAndFlush(message);
            return channelFuture.addListener(listener);
        };
    }

    @Override
    public void startSession() throws TransportException {
        this.startSession(this.syncRequestTimeout);
    }

    private void startSession(long timeout) throws TransportException {
        try {
            this.session.start(timeout);
        } catch (InterruptedException e) {
            throw new TransportException("Interrupted while start session", e);
        }
    }

    @Override
    public boolean send(MessageType messageType, int partition,
                        ByteBuffer buffer) throws TransportException {
        if (!this.checkSendAvailable()) {
            return false;
        }
        this.session.sendAsync(messageType, partition, buffer);
        return true;
    }

    @Override
    public void finishSession() throws TransportException {
        this.finishSession(this.finishSessionTimeout);
    }

    private void finishSession(long timeout) throws TransportException {
        try {
            this.session.finish(timeout);
        } catch (InterruptedException e) {
            throw new TransportException("Interrupted while finish session", e);
        }
    }

    protected boolean checkSendAvailable() {
        return !this.session.blocking() &&
               this.channel.isWritable();
    }

    protected boolean checkAndNoticeSendAvailable() {
        if (this.checkSendAvailable()) {
            this.handler.sendAvailable(this.connectionId);
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        if (this.channel != null) {
            long timeout = this.clientFactory.conf().closeTimeout();
            this.channel.close().awaitUninterruptibly(timeout,
                                                      TimeUnit.MILLISECONDS);
        }
    }

    private class ClientChannelListenerOnWrite
            extends ChannelFutureListenerOnWrite {

        ClientChannelListenerOnWrite() {
            super(NettyTransportClient.this.handler);
        }

        @Override
        public void onSuccess(Channel channel, ChannelFuture future) {
            super.onSuccess(channel, future);
            NettyTransportClient.this.checkAndNoticeSendAvailable();
        }
    }
}
