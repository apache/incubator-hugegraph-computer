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

import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.TransportHandler;
import org.apache.hugegraph.computer.core.network.TransportUtil;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

public class ChannelFutureListenerOnWrite implements ChannelFutureListener {

    private static final Logger LOG =
                         Log.logger(ChannelFutureListenerOnWrite.class);

    private final TransportHandler handler;

    protected ChannelFutureListenerOnWrite(TransportHandler handler) {
        this.handler = handler;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        if (future.isDone()) {
            Channel channel = future.channel();
            this.onDone(channel, future);
        }
    }

    public void onDone(Channel channel, ChannelFuture future) {
        if (future.isSuccess()) {
            this.onSuccess(channel, future);
        } else {
            this.onFailure(channel, future.cause());
        }
    }

    public void onSuccess(Channel channel, ChannelFuture future) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Successfully send data to '{}'",
                      TransportUtil.remoteAddress(channel));
        }
    }

    public void onFailure(Channel channel, Throwable cause) {
        TransportException exception;
        if (cause instanceof TransportException) {
            exception = (TransportException) cause;
        } else {
            exception = new TransportException(
                        "Failed to send data to '%s': %s",
                        cause, TransportUtil.remoteAddress(channel),
                        cause.getMessage());
        }
        ConnectionId connectionId = TransportUtil.remoteConnectionId(channel);
        this.handler.exceptionCaught(exception, connectionId);
    }
}
