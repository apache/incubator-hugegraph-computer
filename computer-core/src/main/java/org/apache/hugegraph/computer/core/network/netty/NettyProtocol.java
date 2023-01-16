/*
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

package org.apache.hugegraph.computer.core.network.netty;

import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.computer.core.network.MessageHandler;
import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.netty.codec.FrameDecoder;
import org.apache.hugegraph.computer.core.network.netty.codec.MessageDecoder;
import org.apache.hugegraph.computer.core.network.netty.codec.MessageEncoder;
import org.apache.hugegraph.computer.core.network.netty.codec.PreciseFrameDecoder;
import org.apache.hugegraph.computer.core.network.session.ServerSession;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateHandler;

/**
 * Defines the server and client channel handlers, i.e. the protocol.
 */
public class NettyProtocol {

    private static final ChannelHandler SLOT_HANDLER = new SLOT_HANDLER();
    private static final int DISABLE_IDLE_TIME = 0;

    private static final ServerIdleHandler SERVER_IDLE_HANDLER =
                                           new ServerIdleHandler();
    private static final HeartbeatHandler HEART_BEAT_HANDLER =
                                          new HeartbeatHandler();

    protected static final String CLIENT_HANDLER_NAME = "networkClientHandler";
    protected static final String SERVER_HANDLER_NAME = "networkServerHandler";

    private final TransportConf conf;

    public NettyProtocol(TransportConf conf) {
        this.conf = conf;
    }

    /**
     * Initialize the server channel handlers.
     *
     * <pre>
     *     +----------------------+
     *     | File / Local Buffer  |
     *     +-----------+----------+
     *                /|\ (2) zero-copy
     * +---------------+---------------------------------------------------+
     * |               |        SERVER CHANNEL PIPELINE                    |
     * |               |                                                   |
     * |    +----------+----------+ (3)write ack +----------------------+  |
     * |    | ServerHandler       |------------->| MessageEncoder       |  |
     * |    +----------+----------+              +-----------+----------+  |
     * |              /|\                                 \|/              |
     * |               |                                   |               |
     * |    +----------+----------+                        |               |
     * |    | MessageDecoder      |                        |               |
     * |    +----------+----------+                        |               |
     * |              /|\                                  |               |
     * |               |                                   |               |
     * |   +-----------+-----------+                       |               |
     * |   | PreciseFrameDecoder   |                       |               |
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

        pipeline.addLast("encoder", MessageEncoder.INSTANCE);

        if (this.conf.recvBufferFileMode()) {
            pipeline.addLast("frameDecoder", new PreciseFrameDecoder());
            pipeline.addLast("decoder", MessageDecoder.INSTANCE_FILE_REGION);
        } else {
            pipeline.addLast("frameDecoder", new FrameDecoder());
            pipeline.addLast("decoder", MessageDecoder.INSTANCE_MEMORY_BUFFER);
        }

        pipeline.addLast("serverIdleStateHandler",
                         this.newServerIdleStateHandler());
        // NOTE: The heartbeatHandler can reuse of a server
        pipeline.addLast("serverIdleHandler", SERVER_IDLE_HANDLER);

        pipeline.addLast(SERVER_HANDLER_NAME,
                         this.newNettyServerHandler(handler));
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

        pipeline.addLast("encoder", MessageEncoder.INSTANCE);

        pipeline.addLast("frameDecoder", new FrameDecoder());

        pipeline.addLast("decoder", MessageDecoder.INSTANCE_MEMORY_BUFFER);

        pipeline.addLast("clientIdleStateHandler",
                         this.newClientIdleStateHandler());
        // NOTE: The heartbeatHandler can reuse
        pipeline.addLast("heartbeatHandler", HEART_BEAT_HANDLER);

        // NOTE: It will be replaced when the client object is initialized!
        pipeline.addLast(CLIENT_HANDLER_NAME, SLOT_HANDLER);

        // Init heartbeat times
        channel.attr(HeartbeatHandler.TIMEOUT_HEARTBEAT_COUNT).set(0);
        channel.attr(HeartbeatHandler.MAX_TIMEOUT_HEARTBEAT_COUNT)
               .set(this.conf.maxTimeoutHeartbeatCount());
    }

    protected void replaceClientHandler(Channel channel,
                                        NettyTransportClient client) {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.replace(CLIENT_HANDLER_NAME, CLIENT_HANDLER_NAME,
                         new NettyClientHandler(client));
    }

    private NettyServerHandler newNettyServerHandler(MessageHandler handler) {
        ServerSession serverSession = new ServerSession(this.conf);
        return new NettyServerHandler(serverSession, handler);
    }

    private IdleStateHandler newServerIdleStateHandler() {
        long timeout = this.conf.serverIdleTimeout();
        return new IdleStateHandler(timeout, DISABLE_IDLE_TIME,
                                    DISABLE_IDLE_TIME, TimeUnit.MILLISECONDS);
    }

    private IdleStateHandler newClientIdleStateHandler() {
        long interval = this.conf.heartbeatInterval();
        return new IdleStateHandler(interval, DISABLE_IDLE_TIME,
                                    DISABLE_IDLE_TIME, TimeUnit.MILLISECONDS);
    }

    @ChannelHandler.Sharable
    private static class SLOT_HANDLER extends ChannelInboundHandlerAdapter {
        // It is an empty handler, only for placeholder to replace it later
    }
}
