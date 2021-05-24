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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.edge.EdgeMessageRecvPartitions;
import com.baidu.hugegraph.computer.core.receiver.message.ComputeMessageRecvPartitions;
import com.baidu.hugegraph.computer.core.receiver.vertex.VertexMessageRecvPartitions;
import com.baidu.hugegraph.computer.core.store.DataFileManager;
import com.baidu.hugegraph.computer.core.worker.MockComputation;
import com.baidu.hugegraph.computer.core.worker.MockMasterComputation;
import com.baidu.hugegraph.config.RpcOptions;
import com.baidu.hugegraph.testutil.Assert;

public class MessageRecvManagerTest {

    private Config config;
    private DataFileManager dataFileManager;
    private MessageRecvManager receiveManager;

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
            ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "1000"
        );
        this.dataFileManager = new DataFileManager();
        this.dataFileManager.init(this.config);
        this.receiveManager = new MessageRecvManager(this.dataFileManager);
        this.receiveManager.init(this.config);
    }

    @After
    public void teardown() {
        this.receiveManager.close(this.config);
        this.dataFileManager.close(this.config);
    }

    @Test
    public void testVertexMessage() {
        for (int i = 0; i < 100; i++) {
            BuffersUtil.mockBufferAndConsume(100, (ManagedBuffer buffer) -> {
                this.receiveManager.handle(MessageType.VERTEX, 0, buffer);
            });
        }
        this.receiveManager.waitReceivedAllMessages();
        VertexMessageRecvPartitions partitions = this.receiveManager
                                                 .removeVertexPartitions();
        partitions.flushAllBuffersAndWaitSorted();

        for (MessageRecvPartition p : partitions.partitions().values()) {
            // Before merge
            Assert.assertEquals(10, p.outputFiles().size());

            // Merge to 1 file
            p.mergeOutputFiles(1);
            Assert.assertEquals(1, p.outputFiles().size());
        }
    }

    @Test
    public void testEdgeMessage() {
        for (int i = 0; i < 89; i++) {
            BuffersUtil.mockBufferAndConsume(100, (ManagedBuffer buffer) -> {
                this.receiveManager.handle(MessageType.EDGE, 0, buffer);
            });
        }
        this.receiveManager.waitReceivedAllMessages();
        EdgeMessageRecvPartitions partitions =
                                  this.receiveManager.removeEdgePartitions();
        partitions.flushAllBuffersAndWaitSorted();

        for (MessageRecvPartition p : partitions.partitions().values()) {
            // Before merge
            Assert.assertEquals(9, p.outputFiles().size());

            // Merge to 1 file
            p.mergeOutputFiles(1);
            Assert.assertEquals(1, p.outputFiles().size());
        }
    }

    @Test
    public void testComputeMessage() {
        // Superstep 0
        this.receiveManager.beforeSuperstep(this.config, 0);
        for (int i = 0; i < 71; i++) {
            BuffersUtil.mockBufferAndConsume(100, (ManagedBuffer buffer) -> {
                this.receiveManager.handle(MessageType.MSG, 0, buffer);
            });
        }
        this.receiveManager.waitReceivedAllMessages();
        this.receiveManager.afterSuperstep(this.config, 0);

        ComputeMessageRecvPartitions partitions = this.receiveManager
                                                  .removeMessagePartitions();
        partitions.flushAllBuffersAndWaitSorted();

        for (MessageRecvPartition p : partitions.partitions().values()) {
            // Before merge
            Assert.assertEquals(8, p.outputFiles().size());
        }
        // Superstep 1
        this.receiveManager.beforeSuperstep(this.config, 1);
        for (int i = 0; i < 51; i++) {
            BuffersUtil.mockBufferAndConsume(100, (ManagedBuffer buffer) -> {
                this.receiveManager.handle(MessageType.MSG, 0, buffer);
            });
        }
        this.receiveManager.waitReceivedAllMessages();
        this.receiveManager.afterSuperstep(this.config, 1);

        partitions = this.receiveManager.removeMessagePartitions();
        partitions.flushAllBuffersAndWaitSorted();

        for (MessageRecvPartition p : partitions.partitions().values()) {
            // Before merge
            Assert.assertEquals(6, p.outputFiles().size());
        }
    }

    @Test
    public void testFinishMessage() {
        // TODO: implement
    }

    @Test
    public void testOtherMessageType() {
        Assert.assertThrows(ComputerException.class, () -> {
            BuffersUtil.mockBufferAndConsume(100, (ManagedBuffer buffer) -> {
                this.receiveManager.handle(MessageType.ACK, 0, buffer);
            });
        }, e -> {
            Assert.assertEquals("Unable handle ManagedBuffer with type 'ACK'",
                                e.getMessage());
        });

    }
}
