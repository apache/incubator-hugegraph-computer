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

package org.apache.hugegraph.computer.core.network.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.network.ClientFactory;
import org.apache.hugegraph.computer.core.network.ClientHandler;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.TransportClient;
import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
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

        this.workerGroup = NettyEventLoopUtil.createEventLoop(
                           this.conf.ioMode(), this.conf.clientThreads(),
                           TransportConf.CLIENT_THREAD_GROUP_NAME);

        this.bootstrap = new Bootstrap();
        this.bootstrap.group(this.workerGroup);
        this.bootstrap
            .channel(NettyEventLoopUtil.clientChannelClass(this.conf.ioMode()));

        this.bootstrap.option(ChannelOption.ALLOCATOR, this.bufAllocator);
        this.bootstrap.option(ChannelOption.TCP_NODELAY, true);
        this.bootstrap.option(ChannelOption.SO_KEEPALIVE,
                              this.conf.tcpKeepAlive());
        this.bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                              this.connectTimeoutMs);

        if (this.conf.sizeReceiveBuffer() > 0) {
            this.bootstrap.option(ChannelOption.SO_RCVBUF,
                                  this.conf.sizeReceiveBuffer());
        }

        if (this.conf.sizeSendBuffer() > 0) {
            this.bootstrap.option(ChannelOption.SO_SNDBUF,
                                  this.conf.sizeSendBuffer());
        }

        // Set low water mark and high water mark for the write buffer.
        WriteBufferWaterMark bufferWaterMark = new WriteBufferWaterMark(
                                               this.conf.writeBufferLowMark(),
                                               this.conf.writeBufferHighMark());
        this.bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                              bufferWaterMark);

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
    public TransportClient createClient(ConnectionId connectionId,
                                        ClientHandler handler)
            throws TransportException {
        InetSocketAddress address = connectionId.socketAddress();
        LOG.debug("Creating new client connection to '{}'", connectionId);

        Channel channel = this.doConnectWithRetries(address,
                                                    this.conf.networkRetries(),
                                                    this.connectTimeoutMs);
        NettyTransportClient client = new NettyTransportClient(channel,
                                                               connectionId,
                                                               this, handler);
        LOG.debug("Successfully created a new client to '{}'", connectionId);
        return client;
    }

    /**
     * Connect to the remote server.
     */
    protected Channel doConnect(InetSocketAddress address, int connectTimeoutMs)
                                throws TransportException {
        E.checkArgumentNotNull(this.bootstrap,
                               "The NettyClientFactory has not been " +
                               "initialized yet");
        long preConnect = System.nanoTime();

        String formatAddress = TransportUtil.formatAddress(address);
        LOG.debug("ConnectTimeout of address [{}] is [{}]", formatAddress,
                  connectTimeoutMs);

        ChannelFuture future = this.bootstrap.connect(address);

        boolean success = future.awaitUninterruptibly(connectTimeoutMs,
                                                      TimeUnit.MILLISECONDS);

        if (!future.isDone()) {
            throw new TransportException(
                  "Create connection to '%s' timeout!", formatAddress);
        }

        if (future.isCancelled()) {
            throw new TransportException(
                  "Create connection to '%s' cancelled by user!",
                  formatAddress);
        }

        if (future.cause() != null) {
            throw new TransportException(
                  "Failed to create connection to '%s', caused by: %s",
                  future.cause(), formatAddress, future.cause().getMessage());
        }

        if (!success || !future.isSuccess()) {
            throw new TransportException(
                  "Failed to create connection to '%s'",
                  formatAddress);
        }

        long postConnect = System.nanoTime();

        LOG.info("Successfully created connection to '{}' after {} ms",
                 formatAddress, (postConnect - preConnect) / 1000000L);
        return future.channel();
    }

    /**
     * Connect to the remote server with retries
     */
    protected Channel doConnectWithRetries(InetSocketAddress address,
                                           int retryNumber,
                                           int connectTimeoutMs)
                                           throws TransportException {
        String formatAddress = TransportUtil.formatAddress(address);
        int tried = 0;
        while (true) {
            try {
                return this.doConnect(address, connectTimeoutMs);
            } catch (IOException e) {
                tried++;
                if (tried > retryNumber) {
                    LOG.warn("Failed to connect to '{}', Giving up after {} " +
                             "retries", formatAddress, retryNumber, e);
                    throw e;
                } else {
                    LOG.debug("Failed to connect to '{}' with retries times " +
                              "{}, Retrying...", formatAddress, tried, e);
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
