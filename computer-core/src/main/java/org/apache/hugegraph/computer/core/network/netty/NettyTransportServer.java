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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.network.IOMode;
import org.apache.hugegraph.computer.core.network.MessageHandler;
import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.TransportServer;
import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

public class NettyTransportServer implements TransportServer, Closeable {

    private static final Logger LOG = Log.logger(NettyTransportServer.class);

    private static final String BOSS_THREAD_GROUP_NAME =
            TransportConf.SERVER_THREAD_GROUP_NAME + "-boss";
    private static final String WORKER_THREAD_GROUP_NAME =
            TransportConf.SERVER_THREAD_GROUP_NAME + "-worker";
    private static final int BOSS_THREADS = 1;

    private final ByteBufAllocator bufAllocator;

    private TransportConf conf;
    private ServerBootstrap bootstrap;
    private ChannelFuture bindFuture;
    private InetSocketAddress bindAddress;

    public NettyTransportServer() {
        this(BufAllocatorFactory.createBufAllocator());
    }

    public NettyTransportServer(ByteBufAllocator bufAllocator) {
        this.bufAllocator = bufAllocator;
    }

    @Override
    public synchronized int listen(Config config,
                                   MessageHandler serverHandler) {
        E.checkArgument(this.bindFuture == null,
                        "The TransportServer has already been listened");
        E.checkArgumentNotNull(serverHandler,
                               "The serverHandler param can't be null");
        final long start = System.currentTimeMillis();

        this.init(config);

        // Child channel pipeline for accepted connections
        NettyProtocol protocol = new NettyProtocol(this.conf);
        ServerChannelInitializer initializer = new ServerChannelInitializer(
                                                   protocol, serverHandler);
        this.bootstrap.childHandler(initializer);

        // Start Server
        this.bindFuture = this.bootstrap.bind().syncUninterruptibly();
        this.bindAddress = (InetSocketAddress)
                           this.bindFuture.channel().localAddress();

        final long duration = System.currentTimeMillis() - start;
        LOG.info("The TransportServer started on address {}, took {} ms",
                 TransportUtil.formatAddress(this.bindAddress), duration);

        return this.bindAddress.getPort();
    }

    private void init(Config config) {
        this.conf = TransportConf.wrapConfig(config);
        this.bootstrap = new ServerBootstrap();

        IOMode ioMode = this.conf.ioMode();

        // Create Netty EventLoopGroup
        EventLoopGroup bossGroup = NettyEventLoopUtil.createEventLoop(
                                   ioMode, BOSS_THREADS,
                                   BOSS_THREAD_GROUP_NAME);
        EventLoopGroup workerGroup = NettyEventLoopUtil.createEventLoop(
                                     ioMode, this.conf.serverThreads(),
                                     WORKER_THREAD_GROUP_NAME);
        this.bootstrap.group(bossGroup, workerGroup);
        this.bootstrap.channel(NettyEventLoopUtil.serverChannelClass(ioMode));

        // Server bind address
        this.bootstrap.localAddress(this.conf.serverAddress(),
                                    this.conf.serverPort());

        // The port can still be bound when the socket is in the TIME_WAIT state
        this.bootstrap.option(ChannelOption.SO_REUSEADDR, true);

        this.bootstrap.option(ChannelOption.ALLOCATOR, this.bufAllocator);
        this.bootstrap.childOption(ChannelOption.ALLOCATOR, this.bufAllocator);
        this.bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        this.bootstrap.childOption(ChannelOption.SO_KEEPALIVE,
                                   this.conf.tcpKeepAlive());

        boolean enableLt = this.conf.epollLevelTriggered();
        // Must be use level trigger mode on zero-copy mode
        if (this.conf.recvBufferFileMode()) {
            enableLt = true;
        }
        // Enable trigger mode for epoll if need
        NettyEventLoopUtil.enableTriggeredMode(ioMode, enableLt,
                                               this.bootstrap);

        if (this.conf.maxSynBacklog() > 0) {
            this.bootstrap.option(ChannelOption.SO_BACKLOG,
                                  this.conf.maxSynBacklog());
        }

        if (this.conf.sizeReceiveBuffer() > 0) {
            this.bootstrap.childOption(ChannelOption.SO_RCVBUF,
                                       this.conf.sizeReceiveBuffer());
        }

        if (this.conf.sizeSendBuffer() > 0) {
            this.bootstrap.childOption(ChannelOption.SO_SNDBUF,
                                       this.conf.sizeSendBuffer());
        }
    }

    public TransportConf conf() {
        return this.conf;
    }

    @Override
    public int port() {
        return this.bindAddress().getPort();
    }

    @Override
    public String ip() {
        InetAddress address = this.bindAddress().getAddress();
        return address == null ? null : address.getHostAddress();
    }

    @Override
    public InetSocketAddress bindAddress() {
        E.checkArgumentNotNull(this.bindAddress,
                               "The TransportServer has not been initialized");
        return this.bindAddress;
    }

    @Override
    public void shutdown() {
        try {
            this.close();
        } catch (IOException e) {
            throw new ComputerException("Failed to shutdown server", e);
        }
    }

    @Override
    public boolean bound() {
        return this.bindFuture != null && this.bindFuture.channel() != null &&
               this.bindFuture.channel().isActive();
    }

    @Override
    public void close() throws IOException {
        if (this.bindFuture != null) {
            long timeout = this.conf.closeTimeout();
            this.bindFuture.channel().close()
                           .awaitUninterruptibly(timeout,
                                                 TimeUnit.MILLISECONDS);
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
        private final NettyProtocol protocol;

        public ServerChannelInitializer(NettyProtocol protocol,
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
