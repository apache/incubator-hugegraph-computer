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

package com.baidu.hugegraph.computer.core.input.hg;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.input.EdgeFetcher;
import com.baidu.hugegraph.computer.core.input.GraphFetcher;
import com.baidu.hugegraph.computer.core.input.InputSplit;
import com.baidu.hugegraph.computer.core.input.VertexFetcher;
import com.baidu.hugegraph.computer.core.rpc.InputSplitRpcService;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.HugeClientBuilder;

public class HugeGraphFetcher implements GraphFetcher {

    private final HugeClient client;
    private final VertexFetcher vertexFetcher;
    private final EdgeFetcher edgeFetcher;
    private final InputSplitRpcService rpcService;

    public HugeGraphFetcher(Config config, InputSplitRpcService rpcService) {
        String url = config.get(ComputerOptions.HUGEGRAPH_URL);
        String graph = config.get(ComputerOptions.HUGEGRAPH_GRAPH_NAME);
        String token = config.get(ComputerOptions.AUTH_TOKEN);
        String usrname = config.get(ComputerOptions.AUTH_USRNAME);
        String passwd = config.get(ComputerOptions.AUTH_PASSWD);
        // TODO: need refact after HugeClient upgrade.. 
        HugeClientBuilder clientBuilder = new HugeClientBuilder(url, graph);

        if (token != null && token.length() != 0) {
            this.client = clientBuilder.build();
            this.client.setAuthContext(token);
        } else if (usrname != null && usrname.length() != 0){
            this.client = clientBuilder.configUser(usrname, passwd).build();
        } else {
            this.client = clientBuilder.build();
        }

        this.vertexFetcher = new HugeVertexFetcher(config, this.client);
        this.edgeFetcher = new HugeEdgeFetcher(config, this.client);
        this.rpcService = rpcService;
    }

    @Override
    public void close() {
        this.client.close();
    }

    @Override
    public VertexFetcher vertexFetcher() {
        return this.vertexFetcher;
    }

    @Override
    public EdgeFetcher edgeFetcher() {
        return this.edgeFetcher;
    }

    @Override
    public InputSplit nextVertexInputSplit() {
        return this.rpcService.nextVertexInputSplit();
    }

    @Override
    public InputSplit nextEdgeInputSplit() {
        return this.rpcService.nextEdgeInputSplit();
    }
}
