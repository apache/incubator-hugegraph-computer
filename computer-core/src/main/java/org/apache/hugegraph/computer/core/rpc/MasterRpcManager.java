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

import java.net.InetSocketAddress;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.rpc.RpcProviderConfig;
import org.apache.hugegraph.rpc.RpcServer;
import org.apache.hugegraph.util.E;

public class MasterRpcManager implements Manager {

    public static final String NAME = "master_rpc";

    private RpcServer rpcServer = null;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        assert this.rpcServer == null;
        this.rpcServer = new RpcServer(config.hugeConfig());
    }

    @Override
    public void close(Config config) {
        this.rpcServer.destroy();
    }

    public void registerInputSplitService(InputSplitRpcService service) {
        RpcProviderConfig serverConfig = this.rpcServer.config();
        serverConfig.addService(InputSplitRpcService.class, service);
    }

    public void registerAggregatorService(AggregateRpcService service) {
        RpcProviderConfig serverConfig = this.rpcServer.config();
        serverConfig.addService(AggregateRpcService.class, service);
    }

    public InetSocketAddress start() {
        E.checkNotNull(this.rpcServer, "rpcServer");
        try {
            this.rpcServer.exportAll();

            return InetSocketAddress.createUnresolved(this.rpcServer.host(),
                                                      this.rpcServer.port());
        } catch (Throwable e) {
            this.rpcServer.destroy();
            throw new ComputerException("Failed to start rpc-server", e);
        }
    }
}
