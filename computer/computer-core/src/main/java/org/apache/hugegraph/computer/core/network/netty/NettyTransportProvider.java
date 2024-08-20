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

package org.apache.hugegraph.computer.core.network.netty;

import org.apache.hugegraph.computer.core.network.ClientFactory;
import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.TransportProvider;
import org.apache.hugegraph.computer.core.network.TransportServer;

import io.netty.buffer.ByteBufAllocator;

public class NettyTransportProvider implements TransportProvider {

    @Override
    public TransportServer createServer(TransportConf conf) {
        ByteBufAllocator bufAllocator =
                         BufAllocatorFactory.createBufAllocator();
        return new NettyTransportServer(bufAllocator);
    }

    @Override
    public ClientFactory createClientFactory(TransportConf conf) {
        ByteBufAllocator bufAllocator =
                         BufAllocatorFactory.createBufAllocator();
        return new NettyClientFactory(conf, bufAllocator);
    }
}
