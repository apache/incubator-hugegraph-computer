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

import static com.baidu.hugegraph.computer.core.network.TransportConf.CLIENT_THREAD_GROUP_NAME;
import static com.baidu.hugegraph.computer.core.network.netty.NettyEventLoopUtil.clientChannelClass;
import static com.baidu.hugegraph.computer.core.network.netty.NettyEventLoopUtil.createEventLoop;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.ClientFactory;
import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.TransportClient;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.TransportHandler;
import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

public class NettyClientFactory implements ClientFactory {

    private static final Logger LOG = Log.logger(NettyClientFactory.class);

    private final TransportConf conf;
    private final ByteBufAllocator bufAllocator;
    private final NettyProtocol protocol;

    private EventLoopGroup workerGroup;
    private Bootstrap bootstrap;
    private int connectTimeoutMs;

    public NettyClientFactory(TransportConf conf) {
        this(conf, BufAllocatorFactory.createBufAllocator());
    }

    public NettyClientFactory(TransportConf conf,
                              ByteBufAllocator bufAllocator) {
        this.conf = conf;
        this.bufAllocator = bufAllocator;
        this.protocol = new NettyProtocol(this.conf);
    }

    @Override
    public synchronized void init() {
        E.checkArgument(this.bootstrap == null,
                        "The NettyClientFactory has already been initialized");
        this.connectTimeoutMs = Math.toIntExact(
                                this.conf.clientConnectionTimeout());

        this.workerGroup = createEventLoop(this.conf.ioMode(),
                                           this.conf.clientThreads(),
                                           CLIENT_THREAD_GROUP_NAME);
        this.bootstrap = new Bootstrap();
        this.bootstrap.group(this.workerGroup);
        this.bootstrap.channel(clientChannelClass(this.conf.ioMode()));

        this.bootstrap.option(ChannelOption.ALLOCATOR, this.bufAllocator);
        this.bootstrap.option(ChannelOption.TCP_NODELAY, true);
        this.bootstrap.option(ChannelOption.SO_KEEPALIVE,
                              this.conf.tcpKeepAlive());
        this.bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                              this.connectTimeoutMs);

        if (this.conf.receiveBuf() > 0) {
            this.bootstrap.option(ChannelOption.SO_RCVBUF,
                                  this.conf.receiveBuf());
        }

        if (this.conf.sendBuf() > 0) {
            this.bootstrap.option(ChannelOption.SO_SNDBUF, this.conf.sendBuf());
        }

        this.bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel channel) {
                NettyClientFactory.this.protocol
                                  .initializeClientPipeline(channel);
            }
        });
    }

    /**
     * Create a new {@link TransportClient} to the remote address.
     */
    @Override
    public TransportClient createClient(ConnectionID connectionID,
                                        TransportHandler handler)
                                        throws IOException {
        InetSocketAddress address = connectionID.socketAddress();
        LOG.debug("Creating new client connection to {}", connectionID);

        Channel channel = this.doConnectWithRetries(address,
                                                    this.conf.networkRetries(),
                                                    this.connectTimeoutMs);
        NettyTransportClient client = new NettyTransportClient(channel,
                                                               connectionID,
                                                               this, handler);
        LOG.debug("Success created a new client to {}", connectionID);
        return client;
    }

    /**
     * Connect to the remote server.
     */
    protected Channel doConnect(InetSocketAddress address, int connectTimeoutMs)
                                throws IOException {
        E.checkArgumentNotNull(this.bootstrap,
                               "The NettyClientFactory has not been " +
                               "initialized yet");
        long preConnect = System.nanoTime();

        String formatAddress = TransportUtil.formatAddress(address);
        LOG.debug("ConnectTimeout of address [{}] is [{}]", formatAddress,
                  connectTimeoutMs);

        ChannelFuture future = this.bootstrap.connect(address);

        boolean connectSuccess = future.awaitUninterruptibly(connectTimeoutMs,
                                                             MILLISECONDS);

        if (!future.isDone()) {
            throw new TransportException(
                  "Create connection to %s timeout!", formatAddress);
        }

        if (future.isCancelled()) {
            throw new TransportException(
                  "Create connection to %s cancelled by user!",
                  formatAddress);
        }

        if (future.cause() != null) {
            throw new TransportException(
                  "Create connection to %s error, cause: %s",
                  future.cause(), formatAddress, future.cause().getMessage());
        }

        if (!connectSuccess || !future.isSuccess()) {
            throw new TransportException(
                  "Create connection to %s error!", formatAddress);
        }

        long postConnect = System.nanoTime();

        LOG.info("Successfully created connection to {} after {} ms",
                 formatAddress, (postConnect - preConnect) / 1000000L);
        return future.channel();
    }

    /**
     * Connect to the remote server with retries
     */
    protected Channel doConnectWithRetries(InetSocketAddress address,
                                           int retryNumber,
                                           int connectTimeoutMs)
                                           throws IOException {
        String formatAddress = TransportUtil.formatAddress(address);
        int tried = 0;
        while (true) {
            try {
                return this.doConnect(address, connectTimeoutMs);
            } catch (IOException e) {
                tried++;
                if (tried > retryNumber) {
                    LOG.warn("Failed to connect to {}, Giving up",
                             formatAddress, e);
                    throw e;
                } else {
                    LOG.debug("Failed to connect to {} with retries times {}," +
                              " Retrying...", formatAddress, tried, e);
                }
            }
        }
    }

    public TransportConf conf() {
        return this.conf;
    }

    protected NettyProtocol protocol() {
        return this.protocol;
    }

    @Override
    public void close() {
        if (this.workerGroup != null && !this.workerGroup.isShuttingDown()) {
            this.workerGroup.shutdownGracefully();
            this.workerGroup = null;
        }
        this.bootstrap = null;
    }
}
