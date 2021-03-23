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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

public class NettyTransportServer implements Transport4Server, Closeable {

    private static final Logger LOG = Log.logger(NettyTransportServer.class);

    private static final String BOSS_THREAD_PREFIX = SERVER_THREAD_GROUP_NAME + "-boss";
    private static final String WORK_THREAD_PREFIX = SERVER_THREAD_GROUP_NAME + "-work";
    private static final int BOSS_THREADS = 1;

    private final TransportConf conf;

    private final TransportProtocol protocol;

    private final ByteBufAllocator byteBufAllocator;

    private ServerBootstrap bootstrap;

    private ChannelFuture bindFuture;

    private InetSocketAddress serverSocketAddress;

    public NettyTransportServer(Config config) {
        this(config, ByteBufAllocatorFactory.createByteBufAllocator());
    }

    public NettyTransportServer(Config config, ByteBufAllocator byteBufAllocator) {
        this.conf = new TransportConf(config);
        this.protocol = new TransportProtocol(conf);
        this.byteBufAllocator = byteBufAllocator;
        init();
    }

    private void init(){
        bootstrap = new ServerBootstrap();

        IOMode ioMode = conf.ioMode();

        // Create Netty EventLoopGroup
        EventLoopGroup bossGroup = createEventLoop(ioMode, BOSS_THREADS,
                                                   BOSS_THREAD_PREFIX);
        EventLoopGroup workerGroup = createEventLoop(ioMode, conf.serverThreads(),
                                                     WORK_THREAD_PREFIX);
        bootstrap.group(bossGroup, workerGroup);
        bootstrap.channel(serverChannelClass(ioMode));

        // Server bind address
        bootstrap.localAddress(conf.serverAddress(), conf.serverPort());

        // The port can still be bound when the socket is in the TIME_WAIT state
        bootstrap.option(ChannelOption.SO_REUSEADDR, true);
        bootstrap.option(ChannelOption.ALLOCATOR, byteBufAllocator);
        bootstrap.childOption(ChannelOption.ALLOCATOR, byteBufAllocator);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, conf.tcpKeepAlive());

        if (conf.backLog() > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, conf.backLog());
        }

        if (conf.receiveBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf());
        }

        if (conf.sendBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf());
        }
    }

    @Override
    public int listen(MessageHandler handler) {
        E.checkState(bindFuture == null,
                     "Netty server has already been listened.");

        // Child channel pipeline for accepted connections
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                LOG.debug("New connection accepted for remote address {}.",
                          ch.remoteAddress());
                ChannelHandler[] channelHandlers =
                        protocol.serverChannelHandlers(handler);
                protocol.initializePipeline(ch, channelHandlers);
            }
        });

        // Start Server
        bindFuture = bootstrap.bind().syncUninterruptibly();

        serverSocketAddress = (InetSocketAddress) bindFuture.channel().localAddress();

        return this.port();
    }

    public TransportConf transportConf(){
        return this.conf;
    }

    public int port() {
        return this.serverSocketAddress().getPort();
    }

    public String hostName() {
        return this.serverSocketAddress().getHostString();
    }

    public InetSocketAddress serverSocketAddress() {
        E.checkArgumentNotNull(serverSocketAddress,
                               "serverSocketAddress not initialized");
        return this.serverSocketAddress;
    }

    public TransportProtocol transportProtocol(){
        return this.protocol;
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
        if (bindFuture != null) {
            bindFuture.channel().close()
                      .awaitUninterruptibly(10, TimeUnit.SECONDS);
            bindFuture = null;
        }
        if (bootstrap != null && bootstrap.config().group() != null) {
            bootstrap.config().group().shutdownGracefully();
        }
        if (bootstrap != null && bootstrap.config().childGroup() != null) {
            bootstrap.config().childGroup().shutdownGracefully();
        }
        bootstrap = null;
    }

    public static NettyTransportServer createNettyTransportServer(Config config) {
        return new NettyTransportServer(config);
    }

    public static NettyTransportServer createNettyTransportServer(Config config,
                                                                  ByteBufAllocator byteBufAllocator) {
        return new NettyTransportServer(config, byteBufAllocator);
    }
}
