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
import java.util.Locale;

import com.baidu.hugegraph.computer.core.common.exception.IllegalArgException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;

import io.netty.channel.epoll.Epoll;

public class TransportConf {

    public static final String SERVER_THREAD_GROUP_NAME =
                               "hugegraph-netty-server";
    public static final String CLIENT_THREAD_GROUP_NAME =
                               "hugegraph-netty-client";
    public static final int NUMBER_CPU_CORES =
                            Runtime.getRuntime().availableProcessors();

    private final Config config;

    public static TransportConf wrapConfig(Config config) {
        return new TransportConf(config);
    }

    protected TransportConf(Config config) {
        this.config = config;
    }

    public InetAddress serverAddress() {
        String host = this.config.get(ComputerOptions.TRANSPORT_SERVER_HOST);
        return TransportUtil.resolvedAddress(host);
    }

    /**
     * A port number of zero will let the system pick up an ephemeral port.
     */
    public int serverPort() {
        return this.config.get(ComputerOptions.TRANSPORT_SERVER_PORT);
    }

    /**
     * IO mode: nio or epoll
     */
    public IOMode ioMode() {
        String ioMode = this.config.get(ComputerOptions.TRANSPORT_IO_MODE)
                                   .toUpperCase(Locale.ROOT);
        switch (ioMode) {
            case "NIO":
                return IOMode.NIO;
            case "EPOLL":
                return IOMode.EPOLL;
            case "AUTO":
                return Epoll.isAvailable() ? IOMode.EPOLL : IOMode.NIO;
            default:
                throw new IllegalArgException("Unknown io_mode: %s", ioMode);
        }
    }

    /**
     * The transport provider
     */
    public TransportProvider transportProvider() {
        return this.config
                   .createObject(ComputerOptions.TRANSPORT_PROVIDER_CLASS);
    }

    /**
     * Enabled EPOLL level trigger
     */
    public boolean epollLevelTriggered() {
        return this.config.get(ComputerOptions.TRANSPORT_EPOLL_LT);
    }

    /**
     * Requested maximum length of the queue of incoming connections. If
     * &lt; 1,
     * the default Netty value of {@link io.netty.util.NetUtil#SOMAXCONN} will
     * be used.
     */
    public int backLog() {
        return this.config.get(ComputerOptions.TRANSPORT_BACKLOG);
    }

    /**
     * Number of threads used in the server EventLoop thread pool. Default to
     * 0, which is CPUs.
     */
    public int serverThreads() {
        int serverThreads = this.config.get(
                            ComputerOptions.TRANSPORT_SERVER_THREADS);
        return serverThreads == 0 ? NUMBER_CPU_CORES : serverThreads;
    }

    public int clientThreads() {
        int clientThreads = this.config.get(
                            ComputerOptions.TRANSPORT_CLIENT_THREADS);
        return clientThreads == 0 ? NUMBER_CPU_CORES : clientThreads;
    }

    /**
     * Receive buffer size (SO_RCVBUF).
     * Note: the optimal size for receive buffer and send buffer should be
     * latency * network_bandwidth.
     * Assuming latency = 1ms, network_bandwidth = 10Gbps
     * buffer size should be ~ 1.25MB
     */
    public int receiveBuf() {
        return this.config.get(ComputerOptions.TRANSPORT_RECEIVE_BUFFER_SIZE);
    }

    public int networkRetries() {
        return this.config.get(ComputerOptions.TRANSPORT_NETWORK_RETRIES);
    }

    public long clientConnectionTimeout() {
        return this.config
                   .get(ComputerOptions.TRANSPORT_CLIENT_CONNECT_TIMEOUT);
    }

    public long closeTimeout() {
        return this.config.get(ComputerOptions.TRANSPORT_CLOSE_TIMEOUT);
    }

    /**
     * The max number of request allowed to unreceived ack.
     * Note: If the number of unreceived ack greater than
     * TRANSPORT_MAX_PENDING_REQUESTS,
     * {@link TransportClient#send} will unavailable
     */
    public int maxPendingRequests() {
        return this.config.get(ComputerOptions.TRANSPORT_MAX_PENDING_REQUESTS);
    }

    /**
     * The minimum number of client unreceived ack.
     */
    public int minPendingRequests() {
        return this.config.get(ComputerOptions.TRANSPORT_MIN_PENDING_REQUESTS);
    }

    /**
     * The minimum interval(in ms) of server reply ack.
     */
    public long minAckInterval() {
        return this.config.get(ComputerOptions.TRANSPORT_MIN_ACK_INTERVAL);
    }

    public int heartbeatInterval() {
        return this.config.get(ComputerOptions.TRANSPORT_HEARTBEAT_INTERVAL);
    }

    public int heartbeatTimeout() {
        return this.config.get(ComputerOptions.TRANSPORT_HEARTBEAT_TIMEOUT);
    }

    public boolean tcpKeepAlive() {
        return this.config.get(ComputerOptions.TRANSPORT_TCP_KEEP_ALIVE);
    }

    public int sendBuf() {
        return this.config.get(ComputerOptions.TRANSPORT_SEND_BUFFER_SIZE);
    }
}
