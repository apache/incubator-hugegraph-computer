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

package org.apache.hugegraph.computer.core.rpc;

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.config.RpcOptions;
import org.apache.hugegraph.rpc.RpcClientProvider;
import org.apache.hugegraph.rpc.RpcConsumerConfig;

public class WorkerRpcManager implements Manager {

    public static final String NAME = "worker_rpc";

    private RpcClientProvider rpcClient = null;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        if (this.rpcClient != null) {
            return;
        }
        this.rpcClient = new RpcClientProvider(config.hugeConfig());
    }

    @Override
    public void close(Config config) {
        this.rpcClient.destroy();
    }

    public InputSplitRpcService inputSplitService() {
        RpcConsumerConfig clientConfig = this.rpcClient.config();
        return clientConfig.serviceProxy(InputSplitRpcService.class);
    }

    public AggregateRpcService aggregateRpcService() {
        RpcConsumerConfig clientConfig = this.rpcClient.config();
        return clientConfig.serviceProxy(AggregateRpcService.class);
    }

    public static void updateRpcRemoteServerConfig(Config config,
                                                   String host, int port) {
        /*
         * Update rpc remote-url to the global config.
         * NOTE: this rpc-server address is from ContainerInfo.masterInfo,
         * the masterInfo has bee got from BSP server.
         */
        String url = host + ":" + port;
        config.hugeConfig().setProperty(RpcOptions.RPC_REMOTE_URL.name(), url);
    }
}
