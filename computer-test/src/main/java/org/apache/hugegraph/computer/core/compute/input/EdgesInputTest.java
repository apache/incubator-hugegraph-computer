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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.compute.FileGraphPartition;
import org.apache.hugegraph.computer.core.compute.MockMessageSender;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.config.EdgeFrequency;
import org.apache.hugegraph.computer.core.config.Null;
import org.apache.hugegraph.computer.core.graph.edge.Edge;
import org.apache.hugegraph.computer.core.graph.edge.Edges;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.core.io.BytesOutput;
import org.apache.hugegraph.computer.core.io.GraphComputeOutput;
import org.apache.hugegraph.computer.core.io.IOFactory;
import org.apache.hugegraph.computer.core.io.StreamGraphOutput;
import org.apache.hugegraph.computer.core.manager.Managers;
import org.apache.hugegraph.computer.core.network.ConnectionId;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.receiver.MessageRecvManager;
import org.apache.hugegraph.computer.core.receiver.ReceiverUtil;
import org.apache.hugegraph.computer.core.sender.MessageSendManager;
import org.apache.hugegraph.computer.core.snapshot.SnapshotManager;
import org.apache.hugegraph.computer.core.sort.flusher.PeekableIterator;
import org.apache.hugegraph.computer.core.sort.sorting.SendSortManager;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.FileManager;
import org.apache.hugegraph.computer.core.store.entry.EntryOutput;
import org.apache.hugegraph.computer.core.store.entry.EntryOutputImpl;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.After;
import org.junit.Test;

public class EdgesInputTest extends UnitTestBase {

    private Config config;
    private Managers managers;

    @After
    public void teardown() {
        if (this.managers != null) {
            this.managers.closeAll(this.config);
        }
    }

    @Test
    public void testSingle() throws IOException {
        this.testEdgeFreq(EdgeFrequency.SINGLE);
    }

    @Test
    public void testSinglePerLabel() throws IOException {
        this.testEdgeFreq(EdgeFrequency.SINGLE_PER_LABEL);
    }

    @Test
    public void testMultiple() throws IOException {
        this.testEdgeFreq(EdgeFrequency.MULTIPLE);
    }

    @Test
    public void testEmptyEdges() {
        EdgesInput.EmptyEdges edges = EdgesInput.EmptyEdges.instance();
        Iterator<Edge> it = edges.iterator();
        Assert.assertFalse(it.hasNext());
        Assert.assertEquals(0, edges.size());
        Assert.assertThrows(ComputerException.class, () -> {
            edges.add(graphFactory().createEdge());
        }, e -> {
            Assert.assertContains("Not support adding edges during computing",
                                  e.getMessage());
        });
    }

    private void testEdgeFreq(EdgeFrequency freq)
                              throws IOException {
        this.config = UnitTestBase.updateWithRequiredOptions(
                ComputerOptions.JOB_ID, "local_001",
                ComputerOptions.JOB_WORKERS_COUNT, "1",
                ComputerOptions.JOB_PARTITIONS_COUNT, "1",
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
                ComputerOptions.INPUT_EDGE_FREQ, freq.name(),
                ComputerOptions.TRANSPORT_RECV_FILE_MODE, "false"
        );
        this.managers = new Managers();
        FileManager fileManager = new FileManager();
        this.managers.add(fileManager);
        SortManager sortManager = new SendSortManager(context());
        this.managers.add(sortManager);

        MessageSendManager sendManager = new MessageSendManager(
                                         context(), sortManager,
                                         new MockMessageSender());
        this.managers.add(sendManager);
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
        ConnectionId connectionId = new ConnectionId(new InetSocketAddress(
                                                     "localhost", 8081),
                                                     0);
        FileGraphPartition partition = new FileGraphPartition(
                                           context(), this.managers, 0);
        receiveManager.onStarted(connectionId);
        add200VertexBuffer((NetworkBuffer buffer) -> {
            receiveManager.handle(MessageType.VERTEX, 0, buffer);
        });
        receiveManager.onFinished(connectionId);
        receiveManager.onStarted(connectionId);
        addEdgeBuffer((NetworkBuffer buffer) -> {
            receiveManager.handle(MessageType.EDGE, 0, buffer);
        }, freq);

        receiveManager.onFinished(connectionId);
        Whitebox.invoke(partition.getClass(), new Class<?>[] {
                                PeekableIterator.class, PeekableIterator.class},
                        "input", partition,
                        receiveManager.vertexPartitions().get(0),
                        receiveManager.edgePartitions().get(0));
        File edgeFile = Whitebox.getInternalState(partition, "edgeFile");
        EdgesInput edgesInput = new EdgesInput(context(), edgeFile);
        edgesInput.init();
        this.checkEdgesInput(edgesInput, freq);
        edgesInput.close();
    }

    private static void add200VertexBuffer(Consumer<NetworkBuffer> consumer)
                                           throws IOException {
        for (long i = 0L; i < 200L; i += 2) {
            Vertex vertex = graphFactory().createVertex();
            vertex.id(BytesId.of(i));
            vertex.properties(graphFactory().createProperties());
            ReceiverUtil.consumeBuffer(writeVertex(vertex), consumer);
        }
    }

    private static byte[] writeVertex(Vertex vertex) throws IOException {
        BytesOutput bytesOutput = IOFactory.createBytesOutput(
                Constants.SMALL_BUF_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(bytesOutput);
        GraphComputeOutput output = new StreamGraphOutput(context(),
                                                          entryOutput);
        output.writeVertex(vertex);
        return bytesOutput.toByteArray();
    }

    private static void addEdgeBuffer(Consumer<NetworkBuffer> consumer,
                                      EdgeFrequency freq) throws IOException {
        for (long i = 0L; i < 200L; i++) {
            Vertex vertex = graphFactory().createVertex();
            vertex.id(BytesId.of(i));
            int count = (int) i;
            if (count == 0) {
                continue;
            }
            Edges edges = graphFactory().createEdges(count);

            for (long j = 0; j < count; j++) {
                Edge edge = graphFactory().createEdge();
                switch (freq) {
                    case SINGLE:
                        edge.targetId(BytesId.of(j));
                        break;
                    case SINGLE_PER_LABEL:
                        edge.label(String.valueOf(j));
                        edge.targetId(BytesId.of(j));
                        break;
                    case MULTIPLE:
                        edge.name(String.valueOf(j));
                        edge.label(String.valueOf(j));
                        edge.targetId(BytesId.of(j));
                        break;
                    default:
                        throw new ComputerException(
                                  "Illegal edge frequency %s", freq);
                }

                Properties properties = graphFactory().createProperties();
                properties.put("p1", new LongValue(i));
                edge.properties(properties);
                edges.add(edge);
            }
            vertex.edges(edges);
            ReceiverUtil.consumeBuffer(writeEdges(vertex, freq), consumer);
        }
    }

    private static byte[] writeEdges(Vertex vertex, EdgeFrequency freq)
                                     throws IOException {
        BytesOutput bytesOutput = IOFactory.createBytesOutput(
                                  Constants.SMALL_BUF_SIZE);
        EntryOutput entryOutput = new EntryOutputImpl(bytesOutput);
        GraphComputeOutput output = new StreamGraphOutput(context(),
                                                          entryOutput);
        Whitebox.setInternalState(output, "frequency", freq);
        output.writeEdges(vertex);
        return bytesOutput.toByteArray();
    }

    private void checkEdgesInput(EdgesInput edgesInput, EdgeFrequency freq)
                                 throws IOException {

        for (long i = 0L; i < 200L; i += 2) {
            Id id = BytesId.of(i);
            ReusablePointer idPointer = idToReusablePointer(id);
            Edges edges = edgesInput.edges(idPointer);
            Iterator<Edge> edgesIt = edges.iterator();
            Assert.assertEquals(i, edges.size());
            for (int j = 0; j < edges.size(); j++) {
                Assert.assertTrue(edgesIt.hasNext());
                Edge edge = edgesIt.next();
                switch (freq) {
                    case SINGLE:
                        Assert.assertEquals(BytesId.of(j), edge.targetId());
                        break;
                    case SINGLE_PER_LABEL:
                        Assert.assertEquals(BytesId.of(j), edge.targetId());
                        Assert.assertEquals(String.valueOf(j), edge.label());
                        break;
                    case MULTIPLE:
                        Assert.assertEquals(BytesId.of(j), edge.targetId());
                        Assert.assertEquals(String.valueOf(j), edge.label());
                        Assert.assertEquals(String.valueOf(j), edge.name());
                        break;
                    default:
                        throw new ComputerException(
                                  "Illegal edge frequency %s", freq);
                }
            }
            Assert.assertFalse(edgesIt.hasNext());
        }
    }

    public static ReusablePointer idToReusablePointer(Id id)
                                                      throws IOException {
        BytesOutput output = IOFactory.createBytesOutput(9);
        id.write(output);
        return new ReusablePointer(output.buffer(), (int) output.position());
    }
}
