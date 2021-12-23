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

package com.baidu.hugegraph.computer.core.compute;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.combiner.EdgeValueCombiner;
import com.baidu.hugegraph.computer.core.combiner.AbstractPointerCombiner;
import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.config.EdgeFrequency;
import com.baidu.hugegraph.computer.core.config.Null;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.BytesId;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.io.BytesOutput;
import com.baidu.hugegraph.computer.core.io.GraphComputeOutput;
import com.baidu.hugegraph.computer.core.io.IOFactory;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphInput;
import com.baidu.hugegraph.computer.core.io.StreamGraphOutput;
import com.baidu.hugegraph.computer.core.manager.Managers;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.receiver.MessageRecvManager;
import com.baidu.hugegraph.computer.core.receiver.ReceiverUtil;
import com.baidu.hugegraph.computer.core.sender.MessageSendManager;
import com.baidu.hugegraph.computer.core.sort.Sorter;
import com.baidu.hugegraph.computer.core.sort.flusher.CombineSubKvOuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.flusher.OuterSortFlusher;
import com.baidu.hugegraph.computer.core.sort.sorting.SendSortManager;
import com.baidu.hugegraph.computer.core.sort.sorting.SortManager;
import com.baidu.hugegraph.computer.core.store.FileManager;
import com.baidu.hugegraph.computer.core.store.hgkvfile.buffer.EntryIterator;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutput;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.EntryOutputImpl;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.KvEntry;
import com.baidu.hugegraph.computer.core.store.hgkvfile.entry.Pointer;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReader;
import com.baidu.hugegraph.computer.core.store.hgkvfile.file.reader.HgkvDirReaderImpl;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Whitebox;

public class ComputeManagerTest extends UnitTestBase {

    private static final Random RANDOM = new Random(1);

    private Config config;
    private Managers managers;
    private ConnectionId connectionId;
    private ComputeManager<?> computeManager;

    @Before
    @SuppressWarnings({ "rawtypes", "unchecked" })
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
            ComputerOptions.INPUT_EDGE_FREQ, "SINGLE"
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
        this.managers.initAll(this.config);
        this.connectionId = new ConnectionId(new InetSocketAddress("localhost",
                                                                   8081),
                                             0);
        Computation<?> computation = this.config.createObject(
                                     ComputerOptions.WORKER_COMPUTATION_CLASS);
        this.computeManager = new ComputeManager(context(), this.managers,
                                                 computation);
    }

    @After
    public void teardown() {
        this.managers.closeAll(this.config);
    }

    @Test
    public void testProcess() throws IOException {
        MessageRecvManager receiveManager = this.managers.get(
                                            MessageRecvManager.NAME);
        receiveManager.onStarted(this.connectionId);
        add200VertexBuffer((ManagedBuffer buffer) -> {
            receiveManager.handle(MessageType.VERTEX, 0, buffer);
        });
        // Partition 1 only has vertex.
        add200VertexBuffer((ManagedBuffer buffer) -> {
            receiveManager.handle(MessageType.VERTEX, 1, buffer);
        });
        receiveManager.onFinished(this.connectionId);
        receiveManager.onStarted(this.connectionId);
        addSingleFreqEdgeBuffer((ManagedBuffer buffer) -> {
            receiveManager.handle(MessageType.EDGE, 0, buffer);
        });
        receiveManager.onFinished(this.connectionId);
        this.computeManager.input();

        // Superstep 0
        receiveManager.beforeSuperstep(this.config, 0);
        receiveManager.onStarted(this.connectionId);
        addMessages((ManagedBuffer buffer) -> {
            receiveManager.handle(MessageType.MSG, 0, buffer);
        });
        receiveManager.onFinished(this.connectionId);
        this.computeManager.compute(null, 0);
        receiveManager.afterSuperstep(this.config, 0);

        // Superstep 1
        this.computeManager.takeRecvedMessages();
        receiveManager.beforeSuperstep(this.config, 1);
        receiveManager.onStarted(this.connectionId);
        receiveManager.onFinished(this.connectionId);
        this.computeManager.compute(null, 1);
        receiveManager.afterSuperstep(this.config, 1);

        // Output
        this.computeManager.output();
    }

    private static void add200VertexBuffer(Consumer<ManagedBuffer> consumer)
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

    private static void addSingleFreqEdgeBuffer(
                        Consumer<ManagedBuffer> consumer) throws IOException {
        for (long i = 0L; i < 200L; i++) {
            Vertex vertex = graphFactory().createVertex();
            vertex.id(BytesId.of(i));
            int count = RANDOM.nextInt(20);
            if (count == 0) {
                continue;
            }
            Edges edges = graphFactory().createEdges(count);

            for (long j = 0; j < count; j++) {
                Edge edge = graphFactory().createEdge();
                edge.targetId(BytesId.of(RANDOM.nextInt(200)));
                Properties properties = graphFactory().createProperties();
                properties.put("p1", new LongValue(i));
                edge.properties(properties);
                edges.add(edge);
            }
            vertex.edges(edges);
            ReceiverUtil.consumeBuffer(writeEdges(vertex, EdgeFrequency.SINGLE),
                                       consumer);
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

    private static void addMessages(Consumer<ManagedBuffer> consumer)
                                    throws IOException {
        for (long i = 0L; i < 200L; i++) {
            int count = RANDOM.nextInt(5);
            for (int j = 0; j < count; j++) {
                Id id = BytesId.of(i);
                IdList message = new IdList();
                message.add(id);
                ReceiverUtil.consumeBuffer(ReceiverUtil.writeMessage(id,
                                                                     message),
                                           consumer);
            }
        }
    }

    @Test
    public void test() throws Exception {
        List<RandomAccessInput> buffers = new ArrayList<>();
        for (long i = 0L; i < 3; i++) {
            Vertex vertex = graphFactory().createVertex();
            vertex.id(BytesId.of(1));
            Edges edges = graphFactory().createEdges(3);

            for (long j = 0; j < 3; j++) {
                Edge edge = graphFactory().createEdge();
                edge.targetId(BytesId.of(j));
                edge.label("hello");
                Properties properties = graphFactory().createProperties();
                properties.put("p1", new LongValue(0));
                edge.properties(properties);
                edges.add(edge);
            }
            vertex.edges(edges);
            byte[] bytes = writeEdges(vertex, EdgeFrequency.SINGLE);
            buffers.add(IOFactory.createBytesInput(bytes));
        }

        SortManager sorter = this.managers.get("send_sort");

        AbstractPointerCombiner combiner = new EdgeValueCombiner(context());
        OuterSortFlusher flusher = new CombineSubKvOuterSortFlusher(combiner,
                                                                    5);
        String path = "~/test/a";
        FileUtils.deleteDirectory(new File(path));
        Sorter sorter1 = Whitebox.getInternalState(sorter, "sorter");
        flusher.sources(buffers.size());
        sorter1.mergeBuffers(buffers, flusher, path, true);

        HgkvDirReader reader = new HgkvDirReaderImpl(path, true);
        EntryIterator iterator = reader.iterator();
        while (iterator.hasNext()) {
            KvEntry next = iterator.next();
            Pointer key = next.key();
            Pointer value = next.value();

            BytesId bytesId = new BytesId();
            bytesId.read(key.input());

            RandomAccessInput input = value.input();
            int count = input.readFixedInt();
            Edges edges = graphFactory().createEdges(count);
            for (int i = 0; i < count; i++) {
                Edge edge = graphFactory().createEdge();
                // Only use targetId as subKey, use props as subValue
                input.readFixedInt();
                edge.targetId(StreamGraphInput.readId(input));
                // Read subValue
                input.readFixedInt();
                Properties props = graphFactory().createProperties();
                props.read(input);
                edge.properties(props);
                edge.label(StreamGraphInput.readLabel(input));
                edges.add(edge);
            }
        }
    }
}
