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

import java.util.concurrent.ThreadFactory;

import org.apache.hugegraph.computer.core.common.exception.IllegalArgException;
import org.apache.hugegraph.computer.core.network.IOMode;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

public class NettyEventLoopUtil {

    private static ThreadFactory createNamedThreadFactory(String prefix) {
        return new DefaultThreadFactory(prefix, true);
    }

    /**
     * Create a Netty EventLoopGroup based on the IOMode.
     */
    public static EventLoopGroup createEventLoop(IOMode mode, int numThreads,
                                                 String prefix) {
        ThreadFactory threadFactory = createNamedThreadFactory(prefix);

        switch (mode) {
            case NIO:
                return new NioEventLoopGroup(numThreads, threadFactory);
            case EPOLL:
                return new EpollEventLoopGroup(numThreads, threadFactory);
            default:
                throw new IllegalArgException("Unknown io mode: " + mode);
        }
    }

    /**
     * Returns the correct (client) SocketChannel class based on IOMode.
     */
    public static Class<? extends Channel> clientChannelClass(IOMode mode) {
        switch (mode) {
            case NIO:
                return NioSocketChannel.class;
            case EPOLL:
                return EpollSocketChannel.class;
            default:
                throw new IllegalArgException("Unknown io mode: " + mode);
        }
    }

    /**
     * Returns the correct ServerSocketChannel class based on IOMode.
     */
    public static Class<? extends ServerChannel> serverChannelClass(
                                                 IOMode mode) {
        switch (mode) {
            case NIO:
                return NioServerSocketChannel.class;
            case EPOLL:
                return EpollServerSocketChannel.class;
            default:
                throw new IllegalArgException("Unknown io mode: " + mode);
        }
    }

    /**
     * Use {@link EpollMode#LEVEL_TRIGGERED} for server bootstrap
     * if level trigger enabled by system properties,
     * otherwise use {@link EpollMode#EDGE_TRIGGERED}.
     */
    public static void enableTriggeredMode(IOMode ioMode, boolean enableLt,
                                           ServerBootstrap serverBootstrap) {
        if (serverBootstrap == null || ioMode != IOMode.EPOLL) {
            return;
        }
        if (enableLt) {
            serverBootstrap.childOption(EpollChannelOption.EPOLL_MODE,
                                        EpollMode.LEVEL_TRIGGERED);
        } else {
            serverBootstrap.childOption(EpollChannelOption.EPOLL_MODE,
                                        EpollMode.EDGE_TRIGGERED);
        }
    }
}
