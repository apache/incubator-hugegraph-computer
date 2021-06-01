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

import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.Iterator;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.store.FileManager;
import com.baidu.hugegraph.computer.core.worker.MockComputation;
import com.baidu.hugegraph.computer.core.worker.MockMasterComputation;
import com.baidu.hugegraph.config.RpcOptions;
import com.baidu.hugegraph.testutil.Assert;

public class MessageRecvManagerTest {

    private Config config;
    private FileManager fileManager;
    private MessageRecvManager receiveManager;
    ConnectionId connectionId = new ConnectionId(
                                InetSocketAddress.createUnresolved("localhost",
                                                                   8081),
                                0);

    @Before
    public void setup() {
        this.config = UnitTestBase.updateWithRequiredOptions(
            RpcOptions.RPC_REMOTE_URL, "127.0.0.1:8090",
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.BSP_LOG_INTERVAL, "30000",
            ComputerOptions.BSP_MAX_SUPER_STEP, "2",
            ComputerOptions.WORKER_COMPUTATION_CLASS,
            MockComputation.class.getName(),
            ComputerOptions.MASTER_COMPUTATION_CLASS,
            MockMasterComputation.class.getName(),
            ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
            ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "1000",
            ComputerOptions.WORKER_WAIT_FINISH_MESSAGES_TIMEOUT, "1000"
        );
        this.fileManager = new FileManager();
        this.fileManager.init(this.config);
        this.receiveManager = new MessageRecvManager(this.fileManager);
        this.receiveManager.init(this.config);
    }

    @After
    public void teardown() {
        this.receiveManager.close(this.config);
        this.fileManager.close(this.config);
    }

    @Test
    public void testVertexAndEdgeMessage() {
        // Send vertex message
        this.receiveManager.onStarted(this.connectionId);
        this.receiveManager.onFinished(this.connectionId);

        // Send edge message
        this.receiveManager.onStarted(this.connectionId);
        this.receiveManager.onFinished(this.connectionId);

        this.receiveManager.waitReceivedAllMessages();
        Map<Integer, Iterator<KeyStore.Entry>> vertexPartitions =
        this.receiveManager.vertexPartitions();
        Map<Integer, Iterator<KeyStore.Entry>> edgePartitions =
        this.receiveManager.edgePartitions();
        Assert.assertEquals(1, vertexPartitions.size());
        Assert.assertEquals(1, edgePartitions.size());
        Iterator<KeyStore.Entry> vertexEntry =
        vertexPartitions.values().iterator().next();
        Assert.assertFalse(vertexEntry.hasNext());
        Iterator<KeyStore.Entry> edgeEntry =
        edgePartitions.values().iterator().next();
        Assert.assertFalse(edgeEntry.hasNext());
    }

    @Test
    public void testComputeMessage() {
        // Superstep 0
        this.receiveManager.beforeSuperstep(this.config, 0);

        this.receiveManager.waitReceivedAllMessages();
        this.receiveManager.afterSuperstep(this.config, 0);

        Map<Integer, Iterator<KeyStore.Entry>> messagePartitions =
        this.receiveManager.messagePartitions();

    }

    @Test
    public void testFinishMessage() {
        // TODO: implement
    }

    @Test
    public void testOtherMessageType() {
        Assert.assertThrows(ComputerException.class, () -> {
            ReceiverUtil.comsumeBuffer(100, (ManagedBuffer buffer) -> {
                this.receiveManager.handle(MessageType.ACK, 0, buffer);
            });
        }, e -> {
            Assert.assertEquals("Unable handle ManagedBuffer with type 'ACK'",
                                e.getMessage());
        });
    }
}
