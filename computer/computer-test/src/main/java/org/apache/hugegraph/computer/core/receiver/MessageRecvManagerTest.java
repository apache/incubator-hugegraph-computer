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

package org.apache.hugegraph.computer.core.receiver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hugegraph.computer.core.combiner.DoubleValueSumCombiner;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.receiver.edge.EdgeMessageRecvPartitionTest;
import org.apache.hugegraph.computer.core.receiver.message.ComputeMessageRecvPartitionTest;
import org.apache.hugegraph.computer.core.receiver.vertex.VertexMessageRecvPartitionTest;
import org.apache.hugegraph.computer.core.snapshot.SnapshotManager;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.sort.sorting.RecvSortManager;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.FileManager;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MessageRecvManagerTest extends UnitTestBase {

    private Config config;
    private FileManager fileManager;
    private SortManager sortManager;
    private MessageRecvManager receiveManager;
    private SnapshotManager snapshotManager;
    private ConnectionId connectionId;

    @Before
    public void setup() {
        this.config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.BSP_MAX_SUPER_STEP, "1",
                ComputerOptions.WORKER_COMBINER_CLASS,
                DoubleValueSumCombiner.class.getName(),
                ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
                ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "100",
                ComputerOptions.WORKER_WAIT_FINISH_MESSAGES_TIMEOUT, "100",
                ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                DoubleValue.class.getName(),
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
        this.fileManager = new FileManager();
        this.fileManager.init(this.config);
        this.sortManager = new RecvSortManager(context());
        this.sortManager.init(this.config);
        this.receiveManager = new MessageRecvManager(context(), this.fileManager, this.sortManager);
        this.snapshotManager = new SnapshotManager(context(), null, receiveManager, null);
        this.receiveManager.init(this.config);
        this.connectionId = new ConnectionId(new InetSocketAddress("localhost", 8081), 0);
    }

    @After
    public void teardown() {
        this.receiveManager.close(this.config);
        this.fileManager.close(this.config);
        this.sortManager.close(this.config);
    }

    @Test
    public void testVertexAndEdgeMessage() throws IOException {
        // Send vertex messages
        this.receiveManager.onStarted(this.connectionId);
        this.receiveManager.onFinished(this.connectionId);
        VertexMessageRecvPartitionTest.addTenVertexBuffer((NetworkBuffer buffer) -> {
            this.receiveManager.handle(MessageType.VERTEX, 0, buffer);
        });

        EdgeMessageRecvPartitionTest.addTenEdgeBuffer((NetworkBuffer buffer) -> {
            this.receiveManager.handle(MessageType.EDGE, 0, buffer);
        });
        // Send edge messages
        this.receiveManager.onStarted(this.connectionId);
        this.receiveManager.onFinished(this.connectionId);

        this.receiveManager.waitReceivedAllMessages();
        Map<Integer, PeekableIterator<KvEntry>> vertexPartitions =
                this.receiveManager.vertexPartitions();
        Map<Integer, PeekableIterator<KvEntry>> edgePartitions =
                this.receiveManager.edgePartitions();
        Assert.assertEquals(1, vertexPartitions.size());
        Assert.assertEquals(1, edgePartitions.size());
        VertexMessageRecvPartitionTest.checkPartitionIterator(vertexPartitions.get(0));
        EdgeMessageRecvPartitionTest.checkTenEdges(edgePartitions.get(0));
    }

    @Test
    public void testComputeMessage() throws IOException {
        // Superstep 0
        this.receiveManager.beforeSuperstep(this.config, 0);
        ComputeMessageRecvPartitionTest.addTwentyCombineMessageBuffer((NetworkBuffer buffer) -> {
            this.receiveManager.handle(MessageType.MSG, 0, buffer);
        });
        this.receiveManager.onFinished(this.connectionId);

        this.receiveManager.waitReceivedAllMessages();
        this.receiveManager.afterSuperstep(this.config, 0);

        Map<Integer, PeekableIterator<KvEntry>> messagePartitions =
                this.receiveManager.messagePartitions();
        Assert.assertEquals(1, messagePartitions.size());
        ComputeMessageRecvPartitionTest.checkTenCombineMessages(messagePartitions.get(0));
    }

    @Test
    public void testOtherMessageType() {
        Assert.assertThrows(ComputerException.class, () -> {
            ReceiverUtil.consumeBuffer(new byte[100], (NetworkBuffer buffer) -> {
                this.receiveManager.handle(MessageType.ACK, 0, buffer);
            });
        }, e -> {
            Assert.assertEquals("Unable handle NetworkBuffer with type 'ACK'",
                                e.getMessage());
        });
    }

    @Test
    public void testNotEnoughFinishMessages() {
        this.receiveManager.beforeSuperstep(this.config, 0);
        Assert.assertThrows(ComputerException.class, () -> {
            this.receiveManager.waitReceivedAllMessages();
        }, e -> {
            Assert.assertContains("finish-messages", e.getMessage());
        });
    }
}
