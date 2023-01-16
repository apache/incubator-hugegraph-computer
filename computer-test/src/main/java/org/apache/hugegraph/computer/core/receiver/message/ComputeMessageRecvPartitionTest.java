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

package org.apache.hugegraph.computer.core.receiver.message;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.combiner.DoubleValueSumCombiner;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.config.Null;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.receiver.ReceiverUtil;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.sort.sorting.RecvSortManager;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.FileManager;
import org.apache.hugegraph.computer.core.store.SuperstepFileGenerator;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class ComputeMessageRecvPartitionTest extends UnitTestBase {

    @Test
    public void testCombineMessageRecvPartition() throws IOException {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1",
                // Make sure all buffers within this limit.
                ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "1000",
                ComputerOptions.WORKER_COMBINER_CLASS,
                DoubleValueSumCombiner.class.getName(),
                ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
                ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "10",
                ComputerOptions.ALGORITHM_MESSAGE_CLASS,
                DoubleValue.class.getName(),
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
        FileUtils.deleteQuietly(new File("data_dir1"));
        FileUtils.deleteQuietly(new File("data_dir2"));
        FileManager fileManager = new FileManager();
        fileManager.init(config);
        SortManager sortManager = new RecvSortManager(context());
        sortManager.init(config);
        SuperstepFileGenerator fileGenerator = new SuperstepFileGenerator(
                                               fileManager, 0);
        ComputeMessageRecvPartition partition = new ComputeMessageRecvPartition(
                                                context(), fileGenerator,
                                                sortManager);
        Assert.assertEquals("msg", partition.type());

        addTwentyCombineMessageBuffer(partition::addBuffer);

        checkTenCombineMessages(partition.iterator());

        fileManager.close(config);
        sortManager.close(config);
    }

    @Test
    public void testNotCombineMessageRecvPartition() throws IOException {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.JOB_PARTITIONS_COUNT, "1",
            ComputerOptions.WORKER_COMBINER_CLASS,
            Null.class.getName(),
            ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
            ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "10",
            ComputerOptions.ALGORITHM_MESSAGE_CLASS, IdList.class.getName(),
            ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
        FileUtils.deleteQuietly(new File("data_dir1"));
        FileUtils.deleteQuietly(new File("data_dir2"));
        FileManager fileManager = new FileManager();
        fileManager.init(config);
        SortManager sortManager = new RecvSortManager(context());
        sortManager.init(config);
        SuperstepFileGenerator fileGenerator = new SuperstepFileGenerator(
                                               fileManager, 0);
        ComputeMessageRecvPartition partition = new ComputeMessageRecvPartition(
                                                context(), fileGenerator,
                                                sortManager);
        Assert.assertEquals("msg", partition.type());

        addTwentyDuplicateIdValueListMessageBuffer(partition::addBuffer);

        checkIdValueListMessages(partition.iterator());

        fileManager.close(config);
        sortManager.close(config);
    }

    public static void addTwentyCombineMessageBuffer(
                       Consumer<NetworkBuffer> consumer)
                       throws IOException {
        for (long i = 0L; i < 10L; i++) {
            for (int j = 0; j < 2; j++) {
                Id id = BytesId.of(i);
                DoubleValue message = new DoubleValue(i);
                ReceiverUtil.consumeBuffer(ReceiverUtil.writeMessage(id,
                                                                     message),
                                           consumer);
            }
        }
    }

    public static void checkTenCombineMessages(PeekableIterator<KvEntry> it)
                                               throws IOException {
        Assert.assertTrue(it.hasNext());
        KvEntry lastEntry = it.next();
        Id lastId = ReceiverUtil.readId(lastEntry.key());
        DoubleValue lastSumValue = new DoubleValue();
        ReceiverUtil.readValue(lastEntry.value(), lastSumValue);
        while (it.hasNext()) {
            KvEntry currentEntry = it.next();
            Id currentId = ReceiverUtil.readId(currentEntry.key());
            DoubleValue currentValue = new DoubleValue();
            ReceiverUtil.readValue(lastEntry.value(), currentValue);
            if (lastId.equals(currentId)) {
                lastSumValue.value(lastSumValue.value() + currentValue.value());
            } else {
                Assert.assertEquals((Long) lastId.asObject() * 2.0D,
                                    lastSumValue.value(), 0.0D);
            }
        }
        Assert.assertEquals((Long) lastId.asObject() * 2.0D,
                            lastSumValue.value(), 0.0D);
    }

    private static void addTwentyDuplicateIdValueListMessageBuffer
                        (Consumer<NetworkBuffer> consumer)
                        throws IOException {
        for (long i = 0L; i < 10L; i++) {
            for (int j = 0; j < 2; j++) {
                Id id = BytesId.of(i);
                IdList message = new IdList();
                message.add(id);
                ReceiverUtil.consumeBuffer(ReceiverUtil.writeMessage(id,
                                                                     message),
                                           consumer);
            }
        }
    }

    private static void checkIdValueListMessages(PeekableIterator<KvEntry> it)
                                                 throws IOException {
        for (long i = 0L; i < 10L; i++) {
            for (int j = 0; j < 2; j++) {
                Assert.assertTrue(it.hasNext());
                KvEntry currentEntry = it.next();
                Id currentId = ReceiverUtil.readId(currentEntry.key());
                Id expectId = BytesId.of(i);
                Assert.assertEquals(expectId, currentId);
                IdList expectMessage = new IdList();
                expectMessage.add(expectId);
                IdList currentValue = new IdList();
                ReceiverUtil.readValue(currentEntry.value(), currentValue);
                Assert.assertEquals(expectMessage, currentValue);
            }
        }
        Assert.assertFalse(it.hasNext());
    }
}
