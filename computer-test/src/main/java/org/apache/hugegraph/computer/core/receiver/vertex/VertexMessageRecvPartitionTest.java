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

package org.apache.hugegraph.computer.core.receiver.vertex;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.combiner.MergeNewPropertiesCombiner;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.StreamGraphInput;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.receiver.ReceiverUtil;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.sort.sorting.RecvSortManager;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.FileManager;
import org.apache.hugegraph.computer.core.store.SuperstepFileGenerator;
import org.apache.hugegraph.computer.core.store.entry.EntryOutput;
import org.apache.hugegraph.computer.core.store.entry.EntryOutputImpl;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.entry.Pointer;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VertexMessageRecvPartitionTest extends UnitTestBase {

    private Config config;
    private VertexMessageRecvPartition partition;
    private FileManager fileManager;
    private SortManager sortManager;

    @Before
    public void setup() {
        this.config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1",
                ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
                ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "20",
                ComputerOptions.HGKV_MERGE_FILES_NUM, "5",
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
        FileUtils.deleteQuietly(new File("data_dir1"));
        FileUtils.deleteQuietly(new File("data_dir2"));
        this.fileManager = new FileManager();
        this.fileManager.init(this.config);
        this.sortManager = new RecvSortManager(context());
        this.sortManager.init(this.config);
        SuperstepFileGenerator fileGenerator = new SuperstepFileGenerator(
                                               this.fileManager,
                                               Constants.INPUT_SUPERSTEP);
        this.partition = new VertexMessageRecvPartition(context(),
                                                        fileGenerator,
                                                        this.sortManager);
    }

    @After
    public void teardown() {
        this.fileManager.close(this.config);
        this.sortManager.close(this.config);
    }

    @Test
    public void testVertexMessageRecvPartition() throws IOException {
        Assert.assertEquals("vertex", this.partition.type());
        Assert.assertEquals(0L, this.partition.totalBytes());
        addTenVertexBuffer(this.partition::addBuffer);

        PeekableIterator<KvEntry> it = this.partition.iterator();
        checkPartitionIterator(it);
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testOverwriteCombiner() throws IOException {
        this.config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1",
                ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
                ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "1000",
                ComputerOptions.HGKV_MERGE_FILES_NUM, "5",
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
        FileUtils.deleteQuietly(new File("data_dir1"));
        FileUtils.deleteQuietly(new File("data_dir2"));
        this.fileManager = new FileManager();
        this.fileManager.init(this.config);
        SuperstepFileGenerator fileGenerator = new SuperstepFileGenerator(
                               this.fileManager,
                               Constants.INPUT_SUPERSTEP);
        this.partition = new VertexMessageRecvPartition(context(),
                                                        fileGenerator,
                                                        this.sortManager);
        addTenVertexBuffer(this.partition::addBuffer);
        addTenVertexBuffer(this.partition::addBuffer);

        checkPartitionIterator(this.partition.iterator());

        this.fileManager.close(this.config);
    }

    @Test
    public void testMergePropertiesCombiner() throws IOException {
        this.config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1",
                ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
                ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "10000",
                ComputerOptions.HGKV_MERGE_FILES_NUM, "5",
                ComputerOptions.WORKER_VERTEX_PROPERTIES_COMBINER_CLASS,
                MergeNewPropertiesCombiner.class.getName(),
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
        FileUtils.deleteQuietly(new File("data_dir1"));
        FileUtils.deleteQuietly(new File("data_dir2"));
        this.fileManager = new FileManager();
        this.fileManager.init(this.config);
        SuperstepFileGenerator fileGenerator = new SuperstepFileGenerator(
                                               this.fileManager,
                                               Constants.INPUT_SUPERSTEP);
        this.partition = new VertexMessageRecvPartition(context(),
                                                        fileGenerator,
                                                        this.sortManager);

        addTwentyDuplicateVertexBuffer(this.partition::addBuffer);

        checkTenVertexWithMergedProperties(this.partition.iterator());

        this.fileManager.close(this.config);
    }

    @Test
    public void testMergeBuffersFailed() {
        addTwoEmptyBuffer(this.partition::addBuffer);

        Assert.assertThrows(ComputerException.class, () -> {
            this.partition.iterator();
        }, e -> {
            Assert.assertContains("Failed to merge 2 buffers to file",
                                  e.getMessage());
        });
    }

    @Test
    public void testEmptyIterator() {
        PeekableIterator<KvEntry> it = this.partition.iterator();
        Assert.assertFalse(it.hasNext());
    }

    public static void addTenVertexBuffer(Consumer<NetworkBuffer> consumer)
                                          throws IOException {
        for (long i = 0L; i < 10L; i++) {
            Vertex vertex = graphFactory().createVertex();
            vertex.id(BytesId.of(i));
            vertex.properties(graphFactory().createProperties());
            ReceiverUtil.consumeBuffer(writeVertex(vertex), consumer);
        }
    }

    private static void addTwentyDuplicateVertexBuffer(
                        Consumer<NetworkBuffer> consumer)
                        throws IOException {
        for (long i = 0L; i < 10L; i++) {
            Vertex vertex = graphFactory().createVertex();
            vertex.id(BytesId.of(i));
            Properties properties = graphFactory().createProperties();
            properties.put("p1", new LongValue(i));
            vertex.properties(properties);

            ReceiverUtil.consumeBuffer(writeVertex(vertex), consumer);
        }

        for (long i = 0L; i < 10L; i++) {
            Vertex vertex = graphFactory().createVertex();
            vertex.id(BytesId.of(i));
            Properties properties = graphFactory().createProperties();
            properties.put("p2", new LongValue(2L * i));
            vertex.properties(properties);

            ReceiverUtil.consumeBuffer(writeVertex(vertex), consumer);
        }
    }

    private static void addTwoEmptyBuffer(Consumer<NetworkBuffer> consumer) {
        for (int i = 0; i < 2; i++) {
            ReceiverUtil.consumeBuffer(new byte[2], consumer);
        }
    }

    private static byte[] writeVertex(Vertex vertex) throws IOException {
        BytesOutput bytesOutput = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(bytesOutput);

        entryOutput.writeEntry(out -> {
            vertex.id().write(out);
        }, out -> {
            out.writeUTF(vertex.label());
            vertex.properties().write(out);
        });

        return bytesOutput.toByteArray();
    }

    private static void checkTenVertexWithMergedProperties(
                        PeekableIterator<KvEntry> it)
                        throws IOException {
        for (long i = 0L; i < 10L; i++) {
            // Assert key
            Assert.assertTrue(it.hasNext());
            KvEntry entry = it.next();
            Id id = ReceiverUtil.readId(entry.key());
            Assert.assertEquals(BytesId.of(i), id);

            // Assert value
            Pointer value = entry.value();
            RandomAccessInput input = value.input();
            long position = input.position();
            input.seek(value.offset());
            String label = StreamGraphInput.readLabel(input);
            Assert.assertEquals("", label);
            Properties properties = graphFactory().createProperties();
            properties.read(input);
            input.seek(position);

            Assert.assertEquals(2, properties.size());
            LongValue v1 = properties.get("p1");
            Assert.assertEquals(new LongValue(i), v1);
            LongValue v2 = properties.get("p2");
            Assert.assertEquals(new LongValue(2L * i), v2);
        }
    }

    public static void checkPartitionIterator(PeekableIterator<KvEntry> it)
                                              throws IOException {
        for (long i = 0L; i < 10L; i++) {
            Assert.assertTrue(it.hasNext());
            KvEntry entry = it.next();
            Id id = ReceiverUtil.readId(entry.key());
            Assert.assertEquals(BytesId.of(i), id);
        }
    }
}
