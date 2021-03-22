/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baidu.hugegraph.computer.core.network;

import java.util.Locale;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;

import io.netty.channel.epoll.Epoll;

public class TransportConf {

    private final Config config;

    public TransportConf(Config config) {
        this.config = config;
    }

    /**
     * IO mode: nio or epoll
     */
    public IOMode ioMode() {
        String ioMode =
                this.config.get(ComputerOptions.TRANSPORT_IO_MODE)
                           .toUpperCase(Locale.ROOT);
        switch (ioMode) {
            case "NIO":
                return IOMode.NIO;
            case "EPOLL":
                return IOMode.EPOLL;
            default:
                return Epoll.isAvailable() ? IOMode.EPOLL : IOMode.NIO;
        }
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
     * 0, which is 2x#cores.
     */
    public int serverThreads() {
        return this.config.get(ComputerOptions.TRANSPORT_SERVER_THREADS);
    }

    public int clientThreads() {
        return this.config.get(ComputerOptions.TRANSPORT_CLIENT_THREADS);
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

    public int sendBuf() {
        return this.config.get(ComputerOptions.TRANSPORT_SEND_BUFFER_SIZE);
    }

    public int clientConnectionTimeout() {
        return this.config
                .get(ComputerOptions.TRANSPORT_CLIENT_CONNECT_TIMEOUT_SECONDS);
    }

    /**
     * The max number of request allowed to unreceived ack.
     * Note: If the number of unreceived ack greater than
     * TRANSPORT_MAX_PENDING_REQUESTS,
     * it will block the client from calling
     * {@link com.baidu.hugegraph.computer.core.network.Transport4Client#send}
     */
    public int maxPendingRequests(){
        return this.config.get(ComputerOptions.TRANSPORT_MAX_PENDING_REQUESTS);
    }

    /**
     * The minimum number of client unreceived ack.
     */
    public int minPendingRequests(){
        return this.config.get(ComputerOptions.TRANSPORT_MIN_PENDING_REQUESTS);
    }

    /**
     * The minimum interval(in ms) of server reply ack.
     */
    public long minACKInterval(){
        return this.config.get(ComputerOptions.TRANSPORT_MIN_ACK_INTERVAL);
    }

    public int heartbeatInterval() {
        return this.config
                .get(ComputerOptions.TRANSPORT_HEARTBEAT_INTERVAL_SECONDS);
    }

    public int heartbeatTimeout() {
        return this.config
                .get(ComputerOptions.TRANSPORT_HEARTBEAT_TIMEOUT_SECONDS);
    }
}
