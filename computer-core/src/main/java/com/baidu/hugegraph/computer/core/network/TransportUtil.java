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

package com.baidu.hugegraph.computer.core.network;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputeException;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;
import io.netty.util.concurrent.DefaultThreadFactory;

public class TransportUtil {

    private static final Logger LOG = Log.logger(TransportUtil.class);

    public static final int NUMBER_CPU_CORES =
                            Runtime.getRuntime().availableProcessors();

    public static ThreadFactory createNamedThreadFactory(String prefix) {
        return new DefaultThreadFactory(prefix, true);
    }

    public static String getRemoteAddress(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            return channel.remoteAddress().toString();
        }
        return "<unknown remote>";
    }

    public static InetAddress resolvedSocketAddress(String host) {
        try {
            return InetAddress.getByName(host);
        } catch (
                UnknownHostException e) {
            throw new ComputeException("Failed to parse address from '%s'", e,
                                       host);
        }
    }

    public static InetSocketAddress resolvedSocketAddress(String host,
                                                          int port) {
        long preResolveHost = System.nanoTime();
        InetSocketAddress resolvedAddress = new InetSocketAddress(host, port);
        long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;

        String status = resolvedAddress.isUnresolved() ? "failed" :
                             "succeed";
        if (hostResolveTimeMs > 2000) {
            LOG.warn("DNS resolution {} for {} took {} ms",
                     status, resolvedAddress, hostResolveTimeMs);
        }
        return resolvedAddress;
    }
}
