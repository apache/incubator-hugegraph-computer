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

package com.baidu.hugegraph.computer.core.network;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * Defines the server and client channel handlers, i.e. the protocol.
 */
public class TransportProtocol {

    private final TransportConf conf;

    public TransportProtocol(TransportConf conf) {
        this.conf = conf;
    }

    /**
     * Returns the server channel handlers.
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
     * |    | RequestHandler      +------------->+ MessageEncoder       |  |
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
     *
     * @return channel handlers
     */
    public ChannelHandler[] serverChannelHandlers(MessageHandler handler) {
        return new ChannelHandler[] {};
    }

    public void initializeServerPipeline(SocketChannel channel,
                                         MessageHandler handler) {
        ChannelPipeline pipeline = channel.pipeline();
        ChannelHandler[] channelHandlers = this.serverChannelHandlers(handler);
        pipeline.addLast(channelHandlers);
    }


    /**
     * Returns the client channel handlers.
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
     * |    | ResponseHandler     |            | MessageEncoder       |    |
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
     *
     * @return channel handlers
     */
    public ChannelHandler[] clientChannelHandlers() {
        return new ChannelHandler[] {};
    }

    public void initializeClientPipeline(SocketChannel channel) {
        ChannelPipeline pipeline = channel.pipeline();
        ChannelHandler[] channelHandlers = this.clientChannelHandlers();
        pipeline.addLast(channelHandlers);
    }
}
