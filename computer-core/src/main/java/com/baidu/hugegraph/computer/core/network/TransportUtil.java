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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputeException;
import com.baidu.hugegraph.computer.core.util.StringEncoding;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class TransportUtil {

    private static final Logger LOG = Log.logger(TransportUtil.class);
    private static final long RESOLVE_TIMEOUT = 2000L;

    public static String remoteAddress(Channel channel) {
        if (channel == null || channel.remoteAddress() == null) {
            return null;
        }
        if (channel.remoteAddress() instanceof InetSocketAddress) {
            return formatAddress((InetSocketAddress) channel.remoteAddress());
        } else {
            return channel.toString();
        }
    }

    public static ConnectionId remoteConnectionId(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            InetSocketAddress address = (InetSocketAddress)
                                        channel.remoteAddress();
            return ConnectionId.parseConnectionId(address.getHostName(),
                                                  address.getPort());
        }
        return null;
    }

    public static InetAddress resolvedAddress(String host) {
        try {
            return InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new ComputeException("Failed to parse address from '%s'", e,
                                       host);
        }
    }

    public static InetSocketAddress resolvedSocketAddress(String host,
                                                          int port) {
        long preResolveHost = System.nanoTime();
        InetSocketAddress resolvedAddress = new InetSocketAddress(host, port);
        long resolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000L;


        if (resolveTimeMs > RESOLVE_TIMEOUT || resolvedAddress.isUnresolved()) {
            String status = resolvedAddress.isUnresolved() ?
                            "failed" : "succeed";
            LOG.warn("DNS resolution {} for '{}' took {} ms",
                     status, resolvedAddress, resolveTimeMs);
        }
        return resolvedAddress;
    }

    public static List<String> getLocalIPAddress() {
        List<String> ips = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> allNetInterfaces =
            NetworkInterface.getNetworkInterfaces();
            InetAddress ip;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = allNetInterfaces.nextElement();
                if (!netInterface.isLoopback() && !netInterface.isVirtual() &&
                    netInterface.isUp()) {
                        Enumeration<InetAddress> addresses =
                        netInterface.getInetAddresses();
                        while (addresses.hasMoreElements()) {
                            ip = addresses.nextElement();
                            if (ip instanceof Inet4Address) {
                                ips.add(ip.getHostAddress());
                            }
                        }
                    }
            }
            return ips;
        } catch (Exception e) {
            throw new ComputeException("Failed to getLocalIPAddress", e);
        }
    }

    /**
     * Format Address, priority use of IP
     */
    public static String formatAddress(InetSocketAddress socketAddress) {
        E.checkNotNull(socketAddress, "socketAddress");
        String host = "";
        InetAddress address = socketAddress.getAddress();
        if (address != null && !socketAddress.isUnresolved()) {
            host = address.getHostAddress();
        } else {
            host = socketAddress.getHostString();
        }
        return String.format("%s:%s", host, socketAddress.getPort());
    }

    public static byte[] encodeString(String str) {
        return StringEncoding.encode(str);
    }

    public static String decodeString(byte[] bytes) {
        return StringEncoding.decode(bytes);
    }

    public static String readString(ByteBuf buf, int length) {
        CharSequence sequence = buf.readCharSequence(length,
                                                     StandardCharsets.UTF_8);
        return sequence == null ? null : (String) sequence;
    }
}
