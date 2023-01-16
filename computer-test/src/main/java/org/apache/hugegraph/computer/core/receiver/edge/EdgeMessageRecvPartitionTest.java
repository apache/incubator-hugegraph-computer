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

package org.apache.hugegraph.computer.core.receiver.edge;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.combiner.MergeNewPropertiesCombiner;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.receiver.ReceiverUtil;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.sort.sorting.RecvSortManager;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.EntryIterator;
import org.apache.hugegraph.computer.core.store.FileManager;
import org.apache.hugegraph.computer.core.store.SuperstepFileGenerator;
import org.apache.hugegraph.computer.core.store.entry.EntriesUtil;
import org.apache.hugegraph.computer.core.store.entry.EntryOutput;
import org.apache.hugegraph.computer.core.store.entry.EntryOutputImpl;
import org.apache.hugegraph.computer.core.store.entry.KvEntry;
import org.apache.hugegraph.computer.core.store.entry.KvEntryWriter;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EdgeMessageRecvPartitionTest extends UnitTestBase {

    private Config config;
    private EdgeMessageRecvPartition partition;
    private FileManager fileManager;
    private SortManager sortManager;

    @Before
    public void setup() {
        this.config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1",
                ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
                ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "100",
                ComputerOptions.HGKV_MERGE_FILES_NUM, "2",
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
        this.partition = new EdgeMessageRecvPartition(context(), fileGenerator,
                                                      this.sortManager);
    }

    @After
    public void teardown() {
        this.fileManager.close(this.config);
        this.sortManager.close(this.config);
    }

    @Test
    public void testEdgeMessageRecvPartition() throws IOException {
        Assert.assertEquals("edge", this.partition.type());

        addTenEdgeBuffer((NetworkBuffer buffer) -> {
            this.partition.addBuffer(buffer);
        });

        checkTenEdges(this.partition.iterator());

        this.fileManager.close(this.config);
    }

    @Test
    public void testOverwriteCombiner() throws IOException {
        Assert.assertEquals("edge", this.partition.type());

        addTenEdgeBuffer(this.partition::addBuffer);
        addTenEdgeBuffer(this.partition::addBuffer);

        checkTenEdges(this.partition.iterator());

        this.fileManager.close(this.config);
    }

    @Test
    public void testNotOverwritePropertiesCombiner() throws IOException {
        this.config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.JOB_PARTITIONS_COUNT, "1",
            ComputerOptions.WORKER_DATA_DIRS, "[data_dir1, data_dir2]",
            ComputerOptions.WORKER_RECEIVED_BUFFERS_BYTES_LIMIT, "10000",
            ComputerOptions.HGKV_MERGE_FILES_NUM, "5",
            ComputerOptions.WORKER_EDGE_PROPERTIES_COMBINER_CLASS,
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
        this.partition = new EdgeMessageRecvPartition(context(), fileGenerator,
                                                      this.sortManager);
        Assert.assertEquals("edge", this.partition.type());

        addTenDuplicateEdgeBuffer(this.partition::addBuffer);

        checkTenEdgesWithCombinedProperties(this.partition.iterator());
    }

    public static void addTenEdgeBuffer(Consumer<NetworkBuffer> consumer)
                                        throws IOException {
        for (long i = 0L; i < 10L; i++) {
            Vertex vertex = graphFactory().createVertex();
            vertex.id(BytesId.of(i));
            Edges edges = graphFactory().createEdges(2);
            for (long j = i + 1; j < i + 3; j++) {
                Edge edge = graphFactory().createEdge();
                edge.targetId(BytesId.of(j));
                Properties properties = graphFactory().createProperties();
                properties.put("p1", new LongValue(i));
                edge.properties(properties);
                edges.add(edge);
            }
            vertex.edges(edges);
            ReceiverUtil.consumeBuffer(writeEdges(vertex), consumer);
        }
    }

    private static void addTenDuplicateEdgeBuffer(
                        Consumer<NetworkBuffer> consumer)
                        throws IOException {
        for (long i = 0L; i < 10L; i++) {
            Vertex vertex = graphFactory().createVertex();
            vertex.id(BytesId.of(i));
            Edges edges = graphFactory().createEdges(2);
            for (long j = i + 1; j < i + 3; j++) {
                Edge edge = graphFactory().createEdge();
                edge.targetId(BytesId.of(j));
                Properties properties = graphFactory().createProperties();
                properties.put("p1", new LongValue(i));
                edge.properties(properties);
                edges.add(edge);
            }
            vertex.edges(edges);
            ReceiverUtil.consumeBuffer(writeEdges(vertex), consumer);
        }

        for (long i = 0L; i < 10L; i++) {
            Vertex vertex = graphFactory().createVertex();
            vertex.id(BytesId.of(i));
            Edges edges = graphFactory().createEdges(2);
            for (long j = i + 1; j < i + 3; j++) {
                Edge edge = graphFactory().createEdge();
                edge.targetId(BytesId.of(j));
                Properties properties = graphFactory().createProperties();
                properties.put("p2", new LongValue(2L * i));
                edge.properties(properties);
                edges.add(edge);
            }
            vertex.edges(edges);
            ReceiverUtil.consumeBuffer(writeEdges(vertex), consumer);
        }
    }


    public static void checkTenEdges(PeekableIterator<KvEntry> it)
                                     throws IOException {
        for (long i = 0L; i < 10L; i++) {
            Assert.assertTrue(it.hasNext());
            KvEntry entry = it.next();
            Id id = ReceiverUtil.readId(entry.key());
            Assert.assertEquals(BytesId.of(i), id);

            EntryIterator subKvIt = EntriesUtil.subKvIterFromEntry(entry);
            for (long j = i + 1; j < i + 3; j++) {
                Assert.assertTrue(subKvIt.hasNext());
                KvEntry subKv = subKvIt.next();
                Id targetId = ReceiverUtil.readId(subKv.key());
                Assert.assertEquals(BytesId.of(j), targetId);

                Properties properties = graphFactory().createProperties();
                ReceiverUtil.readValue(subKv.value(), properties);
                Assert.assertEquals(1, properties.size());
                LongValue v1 = properties.get("p1");
                Assert.assertEquals(new LongValue(i), v1);
            }
        }
        Assert.assertFalse(it.hasNext());
    }

    private static void checkTenEdgesWithCombinedProperties(
                        Iterator<KvEntry> it)
                        throws IOException {
        for (long i = 0L; i < 10L; i++) {
            Assert.assertTrue(it.hasNext());
            KvEntry entry = it.next();
            Id id = ReceiverUtil.readId(entry.key());
            Assert.assertEquals(BytesId.of(i), id);
            EntryIterator subKvIt = EntriesUtil.subKvIterFromEntry(entry);
            for (long j = i + 1; j < i + 3; j++) {
                Assert.assertTrue(subKvIt.hasNext());
                KvEntry subKv = subKvIt.next();
                Id targetId = ReceiverUtil.readId(subKv.key());
                Assert.assertEquals(BytesId.of(j), targetId);
                Properties properties = graphFactory().createProperties();

                ReceiverUtil.readValue(subKv.value(), properties);
                Assert.assertEquals(2, properties.size());
                LongValue v1 = properties.get("p1");
                Assert.assertEquals(new LongValue(i), v1);
                LongValue v2 = properties.get("p2");
                Assert.assertEquals(new LongValue(2L * i), v2);
            }
        }
        Assert.assertFalse(it.hasNext());
    }

    private static byte[] writeEdges(Vertex vertex) throws IOException {
        BytesOutput bytesOutput = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(bytesOutput);

        Id id = vertex.id();
        KvEntryWriter subKvWriter = entryOutput.writeEntry(out -> {
            id.write(out);
        });
        for (Edge edge : vertex.edges()) {
            Id targetId = edge.targetId();
            subKvWriter.writeSubKv(out -> {
                targetId.write(out);
            }, out -> {
                edge.properties().write(out);
            });
        }
        subKvWriter.writeFinish();
        return bytesOutput.toByteArray();
    }
}
