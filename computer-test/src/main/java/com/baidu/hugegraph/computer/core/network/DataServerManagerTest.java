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

import java.net.InetSocketAddress;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvManager;
import com.baidu.hugegraph.computer.core.store.DataFileManager;
import com.baidu.hugegraph.computer.core.worker.MockComputation;
import com.baidu.hugegraph.computer.core.worker.MockMasterComputation;
import com.baidu.hugegraph.config.RpcOptions;
import com.baidu.hugegraph.testutil.Assert;

public class DataServerManagerTest {

    @Test
    public void test() {
        Config config = UnitTestBase.updateWithRequiredOptions(
            RpcOptions.RPC_REMOTE_URL, "127.0.0.1:8090",
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.BSP_LOG_INTERVAL, "30000",
            ComputerOptions.BSP_MAX_SUPER_STEP, "2",
            ComputerOptions.WORKER_COMPUTATION_CLASS,
            MockComputation.class.getName(),
            ComputerOptions.MASTER_COMPUTATION_CLASS,
            MockMasterComputation.class.getName()
        );
        DataFileManager dataFileManager = new DataFileManager();
        dataFileManager.init(config);
        MessageRecvManager receiveManager =
                           new MessageRecvManager(dataFileManager);
        receiveManager.init(config);
        DataServerManager serverManager = new DataServerManager(receiveManager);
        serverManager.init(config);

        Assert.assertEquals(DataServerManager.NAME, serverManager.name());
        InetSocketAddress address = serverManager.address();
        Assert.assertNotEquals(0, address.getPort());
        ConnectionId connectionId = ConnectionId.parseConnectionId(
                                                 address.getHostName(),
                                                 address.getPort());
        receiveManager.channelActive(connectionId);
        receiveManager.channelInactive(connectionId);
        TransportException e = new TransportException("test transport " +
                                                      "exception");
        receiveManager.exceptionCaught(e, connectionId);
        serverManager.close(config);
    }
}
