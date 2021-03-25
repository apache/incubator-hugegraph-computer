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
import static com.baidu.hugegraph.computer.core.network.netty.NettyEventLoopFactory.clientChannelClass;
import static com.baidu.hugegraph.computer.core.network.netty.NettyEventLoopFactory.createEventLoop;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.network.ClientFactory;
import com.baidu.hugegraph.computer.core.network.ConnectionID;
import com.baidu.hugegraph.computer.core.network.Transport4Client;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.TransportProtocol;
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
    private final TransportProtocol protocol;
    private EventLoopGroup workerGroup;
    private Bootstrap bootstrap;


    public NettyClientFactory(TransportConf conf, ByteBufAllocator bufAllocator,
                              TransportProtocol protocol) {
        this.conf = conf;
        this.bufAllocator = bufAllocator;
        this.protocol = protocol;
    }

    @Override
    public synchronized void init() {
        E.checkArgument(this.bootstrap == null,
                        "TransportClientFactory has already been" +
                        " initialization.");
        this.workerGroup = createEventLoop(this.conf.ioMode(),
                                           this.conf.serverThreads(),
                                           CLIENT_THREAD_GROUP_NAME);
        this.bootstrap = new Bootstrap();
        this.bootstrap.group(this.workerGroup)
                      .channel(clientChannelClass(this.conf.ioMode()))
                      .option(ChannelOption.ALLOCATOR, this.bufAllocator)
                      .option(ChannelOption.TCP_NODELAY, true)
                      .option(ChannelOption.SO_KEEPALIVE,
                              this.conf.tcpKeepAlive());

        if (this.conf.receiveBuf() > 0) {
            this.bootstrap.option(ChannelOption.SO_RCVBUF,
                                  this.conf.receiveBuf());
        }

        if (this.conf.sendBuf() > 0) {
            this.bootstrap.option(ChannelOption.SO_SNDBUF,
                                  this.conf.sendBuf());
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
     * Create a new {@link Transport4Client} to the remote address.
     */
    @Override
    public Transport4Client createClient(ConnectionID connectionID) {
        InetSocketAddress address = connectionID.socketAddress();
        LOG.debug("Creating new client connection to {}", connectionID);

        int connectTimeoutMs =
                Math.toIntExact(this.conf.clientConnectionTimeoutMs());
        Channel channel = this.doConnect(address, connectTimeoutMs);
        NettyTransportClient transportClient = new NettyTransportClient(
                                               channel, connectionID,
                                    this, connectTimeoutMs);
        LOG.debug("Success Creating new client connection to {}", connectionID);
        return transportClient;
    }

    /**
     * Connect to the remote server
     */
    protected Channel doConnect(InetSocketAddress address,
                                int connectTimeoutMs) {
        E.checkArgumentNotNull(this.bootstrap, "TransportClientFactory has " +
                                               "not been initialized yet.");
        long preConnect = System.nanoTime();

        LOG.debug("connectTimeout of address [{}] is [{}].", address,
                  connectTimeoutMs);

        this.bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                              connectTimeoutMs);

        ChannelFuture future = this.bootstrap.connect(address);

        boolean connectSuccess = future.awaitUninterruptibly(connectTimeoutMs,
                                                             MILLISECONDS);

        E.checkArgument(connectSuccess && future.isDone(),
                        "Create connection to %s timeout!", address);
        E.checkArgument(!future.isCancelled(), "Create connection to %s " +
                                               "cancelled by user!", address);
        E.checkArgument(future.isSuccess(), "Create connection to %s error!",
                        address);
        long postConnect = System.nanoTime();

        LOG.info("Successfully created connection to {} after {} ms",
                 address, (postConnect - preConnect) / 1000000);
        return future.channel();
    }

    public TransportConf transportConf() {
        return this.conf;
    }

    @Override
    public void close() throws IOException {
        if (this.workerGroup != null && !this.workerGroup.isShuttingDown()) {
            this.workerGroup.shutdownGracefully();
        }
        this.bootstrap = null;
    }
}
