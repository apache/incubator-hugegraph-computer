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

package org.apache.hugegraph.computer.core.compute.input;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

import org.apache.hugegraph.computer.core.compute.MockComputation;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.config.Null;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.manager.Managers;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.receiver.MessageRecvManager;
import org.apache.hugegraph.computer.core.receiver.ReceiverUtil;
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

public class MessageInputTest extends UnitTestBase {

    private Config config;
    private Managers managers;
    private ConnectionId connectionId;

    @Before
    public void setup() {
        this.config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "2",
                ComputerOptions.BSP_MAX_SUPER_STEP, "2",
                ComputerOptions.WORKER_COMBINER_CLASS,
                Null.class.getName(), // Can't combine
                ComputerOptions.ALGORITHM_RESULT_CLASS,
                IdListList.class.getName(),
                ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                IdList.class.getName(),
                ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
                ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "10000",
                ComputerOptions.WORKER_WAIT_FINISH_MESSAGES_TIMEOUT, "1000",
                ComputerOptions.INPUT_MAX_EDGES_IN_ONE_VERTEX, "10",
                ComputerOptions.WORKER_COMPUTATION_CLASS,
                MockComputation.class.getName(),
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );

        this.managers = new Managers();
        FileManager fileManager = new FileManager();
        this.managers.add(fileManager);
        SortManager sortManager = new RecvSortManager(context());
        this.managers.add(sortManager);

        MessageRecvManager receiveManager = new MessageRecvManager(context(),
                                                                   fileManager,
                                                                   sortManager);
        this.managers.add(receiveManager);
        SnapshotManager snapshotManager = new SnapshotManager(context(),
                                                              null,
                                                              receiveManager,
                                                              null);
        this.managers.add(snapshotManager);
        this.managers.initAll(this.config);
        this.connectionId = new ConnectionId(new InetSocketAddress("localhost",
                                                                   8081),
                                             0);
    }

    @After
    public void teardown() {
        this.managers.closeAll(this.config);
    }

    @Test
    public void testMessageInput() throws IOException {
        MessageRecvManager receiveManager = this.managers.get(
                                            MessageRecvManager.NAME);
        receiveManager.onStarted(this.connectionId);

        // Superstep 0
        receiveManager.beforeSuperstep(this.config, 0);
        receiveManager.onStarted(this.connectionId);
        addMessages((NetworkBuffer buffer) -> {
                    receiveManager.handle(MessageType.MSG, 0, buffer);
        });
        receiveManager.onFinished(this.connectionId);
        PeekableIterator<KvEntry> it = receiveManager.messagePartitions()
                                                     .get(0);
        MessageInput<IdList> input = new MessageInput<>(context(), it);
        Map<Id, List<IdList>> expectedMessages = expectedMessages();
        checkMessages(expectedMessages, input);
    }

    private void checkMessages(Map<Id, List<IdList>> expectedMessages,
                               MessageInput<IdList> input) throws IOException {
        for (long i = 0L; i < 200L; i++) {
            List<IdList> messages  = expectedMessages.get(BytesId.of(i));
            Id id = BytesId.of(i);
            ReusablePointer idPointer = EdgesInputTest.idToReusablePointer(id);
            Iterator<IdList> mit = input.iterator(idPointer);
            if (messages == null) {
                Assert.assertFalse(mit.hasNext());
            } else {
                for (int j = 0; j < messages.size();j++) {
                    Assert.assertTrue(mit.hasNext());
                    Assert.assertTrue(messages.contains(mit.next()));
                }
            }
        }
    }

    private static void addMessages(Consumer<NetworkBuffer> consumer)
                                    throws IOException {
        Random random = new Random(1);
        for (long i = 0L; i < 200L; i++) {
            int count = random.nextInt(5);
            for (int j = 0; j < count; j++) {
                Id id = BytesId.of(random.nextInt(200));
                IdList message = new IdList();
                message.add(id);
                ReceiverUtil.consumeBuffer(ReceiverUtil.writeMessage(id,
                                                                     message),
                                           consumer);
            }
        }
    }

    private static Map<Id, List<IdList>> expectedMessages() {
        Random random = new Random(1);
        Map<Id, List<IdList>> globalMessages = new HashMap<>();
        for (long i = 0L; i < 200L; i++) {
            int count = random.nextInt(5);
            for (int j = 0; j < count; j++) {
                Id id = BytesId.of(random.nextInt(200));
                IdList message = new IdList();
                message.add(id);
                List<IdList> messages = globalMessages.computeIfAbsent(
                                             id, nid -> new ArrayList<>());
                messages.add(message);
            }
        }
        return globalMessages;
    }
}
