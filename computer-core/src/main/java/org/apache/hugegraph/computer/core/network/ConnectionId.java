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

import static org.apache.hugegraph.computer.core.network.TransportUtil.resolvedSocketAddress;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.hugegraph.util.E;

/**
 * A {@link ConnectionId} identifies a connection to a remote connection
 * manager by the socket address and a client index. This allows multiple
 * connections to the same worker to be distinguished by their client index.
 */
public class ConnectionId {

    private static final ConcurrentHashMap<String, ConnectionId>
            CONNECTION_ID_CACHE = new ConcurrentHashMap<>();

    private final InetSocketAddress address;
    private final int clientIndex;

    public static ConnectionId parseConnectionId(String host, int port) {
        return parseConnectionId(host, port, 0);
    }

    public static ConnectionId parseConnectionId(String host, int port,
                                                 int clientIndex) {
        String cacheKey = buildCacheKey(host, port, clientIndex);

        Function<String, ConnectionId> resolveAddress = key -> {
            InetSocketAddress socketAddress = resolvedSocketAddress(host, port);
            return new ConnectionId(socketAddress, clientIndex);
        };

        return CONNECTION_ID_CACHE.computeIfAbsent(cacheKey, resolveAddress);
    }

    public ConnectionId(InetSocketAddress address) {
        this(address, 0);
    }

    public ConnectionId(InetSocketAddress address, int clientIndex) {
        E.checkArgument(clientIndex >= 0,
                        "The clientIndex must be >= 0");
        // Use resolved address here
        E.checkArgument(!address.isUnresolved(),
                        "The address must be resolved");
        this.address = address;
        this.clientIndex = clientIndex;
    }

    private static String buildCacheKey(String host, int port,
                                        int clientIndex) {
        return host + ":" + port + "[" + clientIndex + "]";
    }

    public InetSocketAddress socketAddress() {
        return this.address;
    }

    public int clientIndex() {
        return this.clientIndex;
    }

    @Override
    public int hashCode() {
        return this.address.hashCode() + (31 * this.clientIndex);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof ConnectionId)) {
            return false;
        }

        final ConnectionId other = (ConnectionId) obj;
        return other.socketAddress().equals(this.address) &&
               other.clientIndex() == this.clientIndex;
    }

    @Override
    public String toString() {
        return String.format("%s[%d]",
                             TransportUtil.formatAddress(this.address),
                             this.clientIndex);
    }
}
