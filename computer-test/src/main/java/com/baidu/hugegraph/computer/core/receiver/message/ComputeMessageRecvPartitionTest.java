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

package com.baidu.hugegraph.computer.core.receiver.message;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.combiner.DoubleValueSumCombiner;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.config.Null;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.IdValueList;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.ReceiverUtil;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.SorterImpl;
import com.baidu.hugegraph.computer.core.sort.flusher.PeekableIterator;
import com.baidu.hugegraph.computer.core.store.FileManager;
import com.baidu.hugegraph.computer.core.store.SuperstepFileGenerator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutputImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;

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
                ComputerOptions.VALUE_TYPE, ValueType.DOUBLE.name(),
                ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
                ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "10"
        );
        FileUtils.deleteQuietly(new File("data_dir1"));
        FileUtils.deleteQuietly(new File("data_dir2"));
        FileManager fileManager = new FileManager();
        fileManager.init(config);
        Sorter sorter = new SorterImpl(config);
        SuperstepFileGenerator fileGenerator = new SuperstepFileGenerator(
                                               fileManager, 0);
        ComputeMessageRecvPartition partition = new ComputeMessageRecvPartition(
                                                context(), fileGenerator,
                                                sorter);
        Assert.assertEquals(MessageType.MSG.name(), partition.type());

        addTwentyCombineMessageBuffer(partition::addBuffer);

        checkTenCombineMessages(partition.iterator());

        fileManager.close(config);
    }

    @Test
    public void testNotCombineMessageRecvPartition() throws IOException {
        Config config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1",
                ComputerOptions.WORKER_COMBINER_CLASS,
                Null.class.getName(),
                ComputerOptions.VALUE_TYPE, ValueType.DOUBLE.name(),
                ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
                ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "10"
        );
        FileUtils.deleteQuietly(new File("data_dir1"));
        FileUtils.deleteQuietly(new File("data_dir2"));
        FileManager fileManager = new FileManager();
        fileManager.init(config);
        Sorter sorter = new SorterImpl(config);
        SuperstepFileGenerator fileGenerator = new SuperstepFileGenerator(
                                               fileManager, 0);
        ComputeMessageRecvPartition partition = new ComputeMessageRecvPartition(
                                                context(), fileGenerator,
                                                sorter);
        Assert.assertEquals(MessageType.MSG.name(), partition.type());

        addTwentyDuplicateIdValueListMessageBuffer(partition::addBuffer);

        checkIdValueListMessages(partition.iterator());

        fileManager.close(config);
    }

    public static void addTwentyCombineMessageBuffer(
                       Consumer<ManagedBuffer> consumer)
                       throws IOException {
        for (long i = 0L; i < 10L; i++) {
            for (int j = 0; j < 2; j++) {
                Id id = new LongId(i);
                DoubleValue message = new DoubleValue(i);
                ReceiverUtil.comsumeBuffer(writeMessage(id, message), consumer);
            }
        }
    }

    public static void checkTenCombineMessages(PeekableIterator<KvEntry> it)
                                               throws IOException {
        Assert.assertTrue(it.hasNext());
        KvEntry lastEntry = it.next();
        Id lastId = ReceiverUtil.readId(context(), lastEntry.key());
        DoubleValue lastSumValue = new DoubleValue();
        ReceiverUtil.readValue(lastEntry.value(), lastSumValue);
        while (it.hasNext()) {
            KvEntry currentEntry = it.next();
            Id currentId = ReceiverUtil.readId(context(), currentEntry.key());
            DoubleValue currentValue = new DoubleValue();
            ReceiverUtil.readValue(lastEntry.value(), currentValue);
            if (lastId.equals(currentId)) {
                lastSumValue.value(lastSumValue.value() + currentValue.value());
            } else {
                Assert.assertEquals(lastId.asLong() * 2.0D,
                                    lastSumValue.value(), 0.0D);
            }
        }
        Assert.assertEquals(lastId.asLong() * 2.0D,
                            lastSumValue.value(), 0.0D);
    }

    private static void addTwentyDuplicateIdValueListMessageBuffer
                        (Consumer<ManagedBuffer> consumer)
                        throws IOException {
        for (long i = 0L; i < 10L; i++) {
            for (int j = 0; j < 2; j++) {
                Id id = new LongId(i);
                IdValueList message = new IdValueList();
                message.add(id.idValue());
                ReceiverUtil.comsumeBuffer(writeMessage(id, message), consumer);
            }
        }
    }

    private static byte[] writeMessage(Id id, Writable message)
                                       throws IOException {
        BytesOutput bytesOutput = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(bytesOutput);

        entryOutput.writeEntry(out -> {
            out.writeByte(id.type().code());
            id.write(out);
        }, out -> {
            message.write(out);
        });
        return bytesOutput.toByteArray();
    }

    private static void checkIdValueListMessages(PeekableIterator<KvEntry> it)
                                                 throws IOException {
        for (long i = 0L; i < 10L; i++) {
            for (int j = 0; j < 2; j++) {
                Assert.assertTrue(it.hasNext());
                KvEntry currentEntry = it.next();
                Id currentId = ReceiverUtil.readId(context(),
                                                   currentEntry.key());
                Id expectId = new LongId(i);
                Assert.assertEquals(expectId, currentId);
                IdValueList expectMessage = new IdValueList();
                expectMessage.add(expectId.idValue());
                IdValueList currentValue = new IdValueList();
                ReceiverUtil.readValue(currentEntry.value(), currentValue);
                Assert.assertEquals(expectMessage, currentValue);
            }
        }
        Assert.assertFalse(it.hasNext());
    }
}
