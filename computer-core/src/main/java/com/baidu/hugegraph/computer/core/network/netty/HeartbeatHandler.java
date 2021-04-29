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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.network.TransportUtil;
import com.baidu.hugegraph.computer.core.network.message.PingMessage;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;

/**
 * Heart beat triggered.
 * Send ping message
 */
@ChannelHandler.Sharable
public class HeartbeatHandler extends ChannelDuplexHandler {

    private static final Logger LOG = Log.logger(HeartbeatHandler.class);

    public static final AttributeKey<Integer> HEARTBEAT_TIMEOUTS_COUNT =
           AttributeKey.valueOf("HEARTBEAT_TIMEOUTS_COUNT");
    public static final AttributeKey<Integer> MAX_HEARTBEAT_TIMEOUTS_COUNT =
           AttributeKey.valueOf("MAX_HEARTBEAT_TIMEOUTS_COUNT");

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx,
                                   Object event) throws Exception {
        if (event instanceof IdleStateEvent) {
            Channel channel = ctx.channel();
            Integer maxTimeoutsCount = channel
                                       .attr(MAX_HEARTBEAT_TIMEOUTS_COUNT)
                                       .get();
            assert maxTimeoutsCount != null;

            Integer lastTimeoutsCount = channel.attr(HEARTBEAT_TIMEOUTS_COUNT)
                                               .get();
            assert lastTimeoutsCount != null;

            int heartbeatTimeoutsCount = lastTimeoutsCount + 1;

            if (heartbeatTimeoutsCount > maxTimeoutsCount) {
                LOG.info("The number of timeouts for waiting heartbeat " +
                         "response more than the max_heartbeat_timeouts_count" +
                         ",close connection to '{} from client side, count: {}",
                         TransportUtil.remoteAddress(channel),
                         heartbeatTimeoutsCount);

                ctx.close();
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Client IdleStateEvent triggered send ping to" +
                              "'{}', count: {}",
                              TransportUtil.remoteAddress(channel),
                              heartbeatTimeoutsCount);
                }

                ctx.writeAndFlush(PingMessage.INSTANCE)
                   .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

                channel.attr(HEARTBEAT_TIMEOUTS_COUNT)
                       .set(heartbeatTimeoutsCount);
            }
        } else {
            super.userEventTriggered(ctx, event);
        }
    }
}
