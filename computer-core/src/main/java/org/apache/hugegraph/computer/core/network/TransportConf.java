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

import java.net.InetAddress;
import java.util.Locale;

import org.apache.hugegraph.computer.core.common.exception.IllegalArgException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.util.E;

import io.netty.channel.epoll.Epoll;

public class TransportConf {

    public static final String SERVER_THREAD_GROUP_NAME =
                               "transport-netty-server";
    public static final String CLIENT_THREAD_GROUP_NAME =
                               "transport-netty-client";
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

    public int serverThreads() {
        return Math.min(
               this.config.get(ComputerOptions.TRANSPORT_SERVER_THREADS),
               this.maxTransportThreads());
    }

    public int clientThreads() {
        return Math.min(
               this.config.get(ComputerOptions.TRANSPORT_CLIENT_THREADS),
               this.maxTransportThreads());
    }

    private int maxTransportThreads() {
        return this.config.get(ComputerOptions.JOB_WORKERS_COUNT);
    }

    public TransportProvider transportProvider() {
        return this.config
                   .createObject(ComputerOptions.TRANSPORT_PROVIDER_CLASS);
    }

    public boolean recvBufferFileMode() {
        return this.config.get(ComputerOptions.TRANSPORT_RECV_FILE_MODE);
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
     * Whether enabled EPOLL level trigger
     */
    public boolean epollLevelTriggered() {
        return this.config.get(ComputerOptions.TRANSPORT_EPOLL_LT);
    }

    public boolean tcpKeepAlive() {
        return this.config.get(ComputerOptions.TRANSPORT_TCP_KEEP_ALIVE);
    }

    /**
     * Requested maximum length of the queue of incoming connections. If
     * &lt; 1,
     * the default Netty value of {@link io.netty.util.NetUtil#SOMAXCONN} will
     * be used.
     */
    public int maxSynBacklog() {
        return this.config.get(ComputerOptions.TRANSPORT_MAX_SYN_BACKLOG);
    }

    /**
     * Receive buffer size (SO_RCVBUF).
     * Note: the optimal size for receive buffer and send buffer should be
     * latency * network_bandwidth.
     * Assuming latency = 1ms, network_bandwidth = 10Gbps
     * buffer size should be ~ 1.25MB
     */
    public int sizeReceiveBuffer() {
        return this.config.get(ComputerOptions.TRANSPORT_RECEIVE_BUFFER_SIZE);
    }

    public int sizeSendBuffer() {
        return this.config.get(ComputerOptions.TRANSPORT_SEND_BUFFER_SIZE);
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

    public long timeoutSyncRequest() {
        return this.config.get(ComputerOptions.TRANSPORT_SYNC_REQUEST_TIMEOUT);
    }

    /**
     * Timeout of finish session, if less than or equal 0 the default value is
     * TRANSPORT_SYNC_REQUEST_TIMEOUT * TRANSPORT_MAX_PENDING_REQUESTS
     */
    public long timeoutFinishSession() {
        long timeout = this.config.get(
                       ComputerOptions.TRANSPORT_FINISH_SESSION_TIMEOUT);
        return timeout > 0 ? timeout :
               this.config.get(ComputerOptions.TRANSPORT_SYNC_REQUEST_TIMEOUT) *
               this.config.get(ComputerOptions.TRANSPORT_MAX_PENDING_REQUESTS);
    }

    public long writeSocketTimeout() {
        return this.config.get(ComputerOptions.TRANSPORT_WRITE_SOCKET_TIMEOUT);
    }

    public int writeBufferHighMark() {
        return this.config
                   .get(ComputerOptions.TRANSPORT_WRITE_BUFFER_HIGH_MARK);
    }

    public int writeBufferLowMark() {
        return this.config
                   .get(ComputerOptions.TRANSPORT_WRITE_BUFFER_LOW_MARK);
    }

    public int maxPendingRequests() {
        return this.config.get(ComputerOptions.TRANSPORT_MAX_PENDING_REQUESTS);
    }

    public int minPendingRequests() {
        int minPendingReqs = this.config.get(
                             ComputerOptions.TRANSPORT_MIN_PENDING_REQUESTS);

        int maxPendingRequests = this.maxPendingRequests();

        E.checkArgument(minPendingReqs <= maxPendingRequests,
                        "The min_pending_requests(%s) must be less than or " +
                        "equal to the max_pending_requests(%s).",
                        minPendingReqs, maxPendingRequests);
        return minPendingReqs;
    }

    public long minAckInterval() {
        return this.config.get(ComputerOptions.TRANSPORT_MIN_ACK_INTERVAL);
    }

    public long serverIdleTimeout() {
        return this.config.get(ComputerOptions.TRANSPORT_SERVER_IDLE_TIMEOUT);
    }

    public long heartbeatInterval() {
        return this.config.get(ComputerOptions.TRANSPORT_HEARTBEAT_INTERVAL);
    }

    public int maxTimeoutHeartbeatCount() {
        return this.config
                   .get(ComputerOptions.TRANSPORT_MAX_TIMEOUT_HEARTBEAT_COUNT);
    }
}
