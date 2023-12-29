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

package org.apache.hugegraph.computer.core.network;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.util.StringEncodeUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.DefaultMaxBytesRecvByteBufAllocator;

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
        if (channel == null || channel.remoteAddress() == null) {
            return null;
        }
        InetSocketAddress address = (InetSocketAddress)
                                    channel.remoteAddress();
        return ConnectionId.parseConnectionId(address.getHostName(),
                                              address.getPort());
    }

    public static InetAddress resolvedAddress(String host) {
        try {
            return InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new ComputerException("Failed to parse address from '%s'", e,
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

    public static String host(InetSocketAddress socketAddress) {
        InetAddress address = socketAddress.getAddress();
        if (address != null) {
            return address.getHostAddress();
        }
        return socketAddress.getHostName();
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
            throw new ComputerException("Failed to getLocalIPAddress", e);
        }
    }

    /**
     * Format Address, priority use of IP
     */
    public static String formatAddress(InetSocketAddress socketAddress) {
        E.checkNotNull(socketAddress, "socketAddress");
        InetAddress address = socketAddress.getAddress();
        String host;
        if (address != null && !socketAddress.isUnresolved()) {
            host = address.getHostAddress();
        } else {
            host = socketAddress.getHostString();
        }
        return String.format("%s:%s", host, socketAddress.getPort());
    }

    public static String readString(ByteBuf buf) {
        int length = buf.readInt();
        E.checkArgument(length >= 0,
                        "The length mast be >= 0, but got %s", length);
        if (length == 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        return StringEncodeUtil.decode(bytes);
    }

    public static void writeString(ByteBuf buf, String value) {
        E.checkArgumentNotNull(value, "value");
        byte[] encoded = StringEncodeUtil.encode(value);
        buf.writeInt(encoded.length);
        buf.writeBytes(encoded);
    }

    public static void setMaxBytesPerRead(Channel channel, int length) {
        DefaultMaxBytesRecvByteBufAllocator recvByteBufAllocator =
                                            channel.config()
                                                   .getRecvByteBufAllocator();
        if (recvByteBufAllocator.maxBytesPerIndividualRead() != length) {
            recvByteBufAllocator.maxBytesPerReadPair(length, length);
        }
    }
}
