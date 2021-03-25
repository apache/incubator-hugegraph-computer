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

import static com.baidu.hugegraph.computer.core.network.TransportConf.SERVER_THREAD_GROUP_NAME;
import static com.baidu.hugegraph.computer.core.network.netty.NettyEventLoopFactory.createEventLoop;
import static com.baidu.hugegraph.computer.core.network.netty.NettyEventLoopFactory.serverChannelClass;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputeException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.Transport4Server;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.TransportProtocol;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

public class NettyTransportServer implements Transport4Server, Closeable {

    private static final Logger LOG = Log.logger(NettyTransportServer.class);

    private static final String BOSS_THREAD_GROUP_NAME =
            SERVER_THREAD_GROUP_NAME + "-boss";
    private static final String WORKER_THREAD_GROUP_NAME =
            SERVER_THREAD_GROUP_NAME + "-worker";
    private static final int BOSS_THREADS = 1;

    private final ByteBufAllocator bufAllocator;

    private TransportConf conf;
    private ServerBootstrap bootstrap;
    private ChannelFuture bindFuture;
    private InetSocketAddress bindSocketAddress;

    private NettyTransportServer(ByteBufAllocator bufAllocator) {
        this.bufAllocator = bufAllocator;
    }

    public static NettyTransportServer newNettyTransportServer() {
        return new NettyTransportServer(
               ByteBufAllocatorFactory.createByteBufAllocator());
    }

    public static NettyTransportServer newNettyTransportServer(
                                       ByteBufAllocator bufAllocator) {
        return NettyTransportServer.newNettyTransportServer(bufAllocator);
    }

    @Override
    public synchronized int listen(Config config, MessageHandler handler) {
        E.checkArgument(this.bindFuture == null,
                        "Netty server has already been listened.");

        this.init(config);

        // Child channel pipeline for accepted connections
        TransportProtocol protocol = new TransportProtocol(this.conf);
        this.bootstrap.childHandler(new ServerChannelInitializer(protocol,
                                                                 handler));

        // Start Server
        this.bindFuture = this.bootstrap.bind().syncUninterruptibly();
        this.bindSocketAddress = (InetSocketAddress)
                                 this.bindFuture.channel().localAddress();

        LOG.info("Transport server started on SocketAddress {}.",
                 this.bindSocketAddress);
        return this.bindSocketAddress().getPort();
    }

    private void init(Config config) {
        this.conf = new TransportConf(config);
        this.bootstrap = new ServerBootstrap();

        IOMode ioMode = this.conf.ioMode();

        // Create Netty EventLoopGroup
        EventLoopGroup bossGroup = createEventLoop(ioMode, BOSS_THREADS,
                                                   BOSS_THREAD_GROUP_NAME);
        EventLoopGroup workerGroup = createEventLoop(ioMode,
                                                     this.conf.serverThreads(),
                                                     WORKER_THREAD_GROUP_NAME);
        this.bootstrap.group(bossGroup, workerGroup);
        this.bootstrap.channel(serverChannelClass(ioMode));

        // Server bind address
        this.bootstrap.localAddress(this.conf.serverAddress(),
                                    this.conf.serverPort());

        // The port can still be bound when the socket is in the TIME_WAIT state
        this.bootstrap.option(ChannelOption.SO_REUSEADDR,
                              true);

        this.bootstrap.option(ChannelOption.ALLOCATOR, this.bufAllocator);
        this.bootstrap.childOption(ChannelOption.ALLOCATOR, this.bufAllocator);
        this.bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        this.bootstrap.childOption(ChannelOption.SO_KEEPALIVE,
                                   this.conf.tcpKeepAlive());

        if (this.conf.backLog() > 0) {
            this.bootstrap.option(ChannelOption.SO_BACKLOG,
                                  this.conf.backLog());
        }

        if (this.conf.receiveBuf() > 0) {
            this.bootstrap.childOption(ChannelOption.SO_RCVBUF,
                                       this.conf.receiveBuf());
        }

        if (this.conf.sendBuf() > 0) {
            this.bootstrap.childOption(ChannelOption.SO_SNDBUF,
                                       this.conf.sendBuf());
        }
    }

    public TransportConf transportConf() {
        return this.conf;
    }

    public int port() {
        return this.bindSocketAddress().getPort();
    }

    public String host() {
        return this.bindSocketAddress().getHostString();
    }

    public InetSocketAddress bindSocketAddress() {
        E.checkArgumentNotNull(this.bindSocketAddress,
                               "bindSocketAddress not initialized");
        return this.bindSocketAddress;
    }

    @Override
    public void stop() {
        try {
            this.close();
        } catch (IOException e) {
            throw new ComputeException("Failed to stop server", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.bindFuture != null) {
            this.bindFuture.channel().close()
                           .awaitUninterruptibly(10, TimeUnit.SECONDS);
            this.bindFuture = null;
        }
        if (this.bootstrap != null && this.bootstrap.config().group() != null) {
            this.bootstrap.config().group().shutdownGracefully();
        }
        if (this.bootstrap != null &&
            this.bootstrap.config().childGroup() != null) {
            this.bootstrap.config().childGroup().shutdownGracefully();
        }
        this.bootstrap = null;
    }

    private static class ServerChannelInitializer
                   extends ChannelInitializer<SocketChannel> {

        private final MessageHandler handler;
        private final TransportProtocol protocol;

        public ServerChannelInitializer(TransportProtocol protocol,
                                        MessageHandler handler) {
            this.handler = handler;
            this.protocol = protocol;
        }

        @Override
        public void initChannel(SocketChannel channel) {
            this.protocol.initializeServerPipeline(channel, this.handler);
        }
    }
}
