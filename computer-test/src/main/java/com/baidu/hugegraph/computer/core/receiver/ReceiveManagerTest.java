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

package com.baidu.hugegraph.computer.core.receiver;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.buffer.NettyManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.store.DataFileManager;
import com.baidu.hugegraph.computer.core.util.StringEncoding;
import com.baidu.hugegraph.computer.core.worker.MockComputation;
import com.baidu.hugegraph.computer.core.worker.MockMasterComputation;
import com.baidu.hugegraph.config.RpcOptions;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ReceiveManagerTest {

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
        ReceiveManager receiveManager = new ReceiveManager(dataFileManager);
        receiveManager.init(config);

        String testData = "test data";
        byte[] bytesSource = StringEncoding.encode(testData);
        ByteBuf buf = Unpooled.directBuffer(bytesSource.length);
        try {
            buf = buf.writeBytes(bytesSource);
            NettyManagedBuffer buff = new NettyManagedBuffer(buf);
            receiveManager.handle(MessageType.VERTEX, 0, buff);
        } finally {
            buf.release();
        }
        dataFileManager.close(config);
        receiveManager.close(config);
    }
}
