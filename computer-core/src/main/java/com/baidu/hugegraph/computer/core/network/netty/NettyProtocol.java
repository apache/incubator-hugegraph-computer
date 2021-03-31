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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.computer.core.network.netty;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.netty.codec.FrameDecoder;
import com.baidu.hugegraph.computer.core.network.netty.codec.MessageDecoder;
import com.baidu.hugegraph.computer.core.network.netty.codec.MessageEncoder;
import com.baidu.hugegraph.computer.core.network.session.ServerSession;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Defines the server and client channel handlers, i.e. the protocol.
 */
public class NettyProtocol {

    private static final Logger LOG = Log.logger(NettyProtocol.class);

    private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
    private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;
    private static final ServerIdleHandler SERVER_IDLE_HANDLER =
            new ServerIdleHandler();
    private static final HeartBeatHandler HEART_BEAT_HANDLER =
            new HeartBeatHandler();

    private static final ChannelHandler SLOT_CHANNEL_HANDLER =
            new ChannelDuplexHandler();

    private static final String CLIENT_HANDLER_NAME = "clientHandler";

    private final TransportConf conf;

    public NettyProtocol(TransportConf conf) {
        this.conf = conf;
    }

    /**
     * Initialize the server channel handlers.
     *
     * <pre>
     *     +----------------------+
     *     | LocalBuffer          |
     *     +-----------+----------+
     *                /|\ (2)handle message
     * +---------------+---------------------------------------------------+
     * |               |        SERVER CHANNEL PIPELINE                    |
     * |               |                                                   |
     * |    +----------+----------+ (3)write ack +----------------------+  |
     * |    | ServerHandler       +------------->+ MessageEncoder       |  |
     * |    +----------+----------+              +-----------+----------+  |
     * |              /|\                                 \|/              |
     * |               |                                   |               |
     * |    +----------+----------+                        |               |
     * |    | MessageDecoder      |                        |               |
     * |    +----------+----------+                        |               |
     * |              /|\                                  |               |
     * |               |                                   |               |
     * |   +-----------+-----------+                       |               |
     * |   | FrameDecoder          |                       |               |
     * |   +-----------+-----------+                       |               |
     * |              /|\                                  |               |
     * +---------------+-----------------------------------+---------------+
     * |               | (1) read request                 \|/              |
     * +---------------+-----------------------------------+---------------+
     * |               |                                   |               |
     * |       [ Socket.read() ]                    [ Socket.write() ]     |
     * |                                                                   |
     * |  Netty Internal I/O Threads (Transport Implementation)            |
     * +-------------------------------------------------------------------+
     * </pre>
     */
    protected void initializeServerPipeline(Channel channel,
                                            MessageHandler handler) {
        ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast("encoder", ENCODER);

        pipeline.addLast("frameDecoder", new FrameDecoder());

        pipeline.addLast("decoder", DECODER);

        int timeout = this.conf.heartbeatTimeout();
        pipeline.addLast("serverIdleStateHandler",
                         new IdleStateHandler(0, 0,
                                              timeout, TimeUnit.SECONDS));
        // NOTE: The heartBeatHandler can reuse of a server
        pipeline.addLast("serverIdleHandler", SERVER_IDLE_HANDLER);

        pipeline.addLast("serverHandler", this.createRequestHandler(handler));
    }


    /**
     * Initialize the client channel handlers.
     *
     * <pre>
     *                                         +----------------------+
     *                                         | request client       |
     *                                         +-----------+----------+
     *                                                     | (1) send message
     * +-------------------------------------------------------------------+
     * |                        CLIENT CHANNEL PIPELINE    |               |
     * |                                                  \|/              |
     * |    +---------------------+            +----------------------+    |
     * |    | ClientHandler       |            | MessageEncoder       |    |
     * |    +----------+----------+            +-----------+----------+    |
     * |              /|\                                 \|/              |
     * |               |                                   |               |
     * |    +----------+----------+                        |               |
     * |    | MessageDecoder      |                        |               |
     * |    +----------+----------+                        |               |
     * |              /|\                                  |               |
     * |               |                                   |               |
     * |   +-----------+-----------+                       |               |
     * |   | FrameDecoder          |                       |               |
     * |   +-----------+-----------+                       |               |
     * |              /|\                                  |               |
     * +---------------+-----------------------------------+---------------+
     * |               | (3) server response              \|/ (2) client request
     * +---------------+-----------------------------------+---------------+
     * |               |                                   |               |
     * |       [ Socket.read() ]                    [ Socket.write() ]     |
     * |                                                                   |
     * |  Netty Internal I/O Threads (Transport Implementation)            |
     * +-------------------------------------------------------------------+
     * </pre>
     */
    protected void initializeClientPipeline(Channel channel) {
        ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast("encoder", ENCODER);

        pipeline.addLast("frameDecoder", new FrameDecoder());

        pipeline.addLast("decoder", DECODER);

        int interval = this.conf.heartbeatInterval();
        pipeline.addLast("clientIdleStateHandler",
                         new IdleStateHandler(interval, interval,
                                              0, TimeUnit.SECONDS));
        // NOTE: The heartBeatHandler can reuse
        pipeline.addLast("heartBeatHandler", HEART_BEAT_HANDLER);

        // NOTE: It will be replaced when the client object is initialized!
        pipeline.addLast(CLIENT_HANDLER_NAME, SLOT_CHANNEL_HANDLER);
    }

    protected void replaceClientHandler(Channel channel,
                                        NettyTransportClient client) {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.replace(CLIENT_HANDLER_NAME, CLIENT_HANDLER_NAME,
                         new NettyClientHandler(client));
    }

    private NettyServerHandler createRequestHandler(MessageHandler handler) {
        ServerSession serverSession = new ServerSession();
        return new NettyServerHandler(serverSession, handler);
    }
}
