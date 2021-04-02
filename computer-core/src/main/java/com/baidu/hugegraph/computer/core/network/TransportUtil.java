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
import java.util.Enumeration;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputeException;
import com.baidu.hugegraph.util.Log;

import io.netty.channel.Channel;

public class TransportUtil {

    private static final Logger LOG = Log.logger(TransportUtil.class);

    public static final int NUMBER_CPU_CORES =
                            Runtime.getRuntime().availableProcessors();

    public static String remoteAddress(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            return formatAddress((InetSocketAddress) channel.remoteAddress());
        }
        return "<unknown remote>";
    }

    public static ConnectionID remoteConnectionID(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            InetSocketAddress address = (InetSocketAddress)
                                        channel.remoteAddress();
            return ConnectionID.parseConnectionID(address.getHostName(),
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

        if (resolveTimeMs > 2000L) {
            String status = resolvedAddress.isUnresolved() ?
                            "failed" : "succeed";
            LOG.warn("DNS resolution {} for {} took {} ms",
                     status, resolvedAddress, resolveTimeMs);
        }
        return resolvedAddress;
    }

    public static String getLocalIPAddress() {
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
                                return ip.getHostAddress();
                            }
                        }
                    }
            }
        } catch (Exception e) {
            throw new ComputeException("Failed to getLocalIPAddress");
        }
        return "<unknown IP>";
    }

    /**
     * Format Address, priority use of IP
     */
    public static String formatAddress(InetSocketAddress socketAddress) {
        if (socketAddress != null) {
            String host = "";
            InetAddress address = socketAddress.getAddress();
            if (address != null && !socketAddress.isUnresolved()) {
                host = address.getHostAddress();
            } else {
                host = socketAddress.getHostString();
            }
            return String.format("<%s:%s>", host, socketAddress.getPort());
        }
        return "<unknown address>";
    }

    public static byte[] encodeString(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static String decodeString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
