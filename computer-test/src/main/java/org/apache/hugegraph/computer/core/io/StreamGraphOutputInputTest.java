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

package org.apache.hugegraph.computer.core.io;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.common.Constants;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.EdgeFrequency;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.properties.Properties;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class StreamGraphOutputInputTest extends UnitTestBase {

    @Test
    public void testWriteReadVertex() throws Exception {
        Id longId = BytesId.of(100L);
        LongValue longValue = new LongValue(999L);
        Vertex vertex = graphFactory().createVertex(longId, longValue);
        Properties properties = graphFactory().createProperties();
        properties.put("age", new LongValue(20L));
        vertex.properties(properties);
        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                Constants.SMALL_BUF_SIZE)) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeVertex(vertex);
            bytes = bao.toByteArray();
        }

        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
            assertVertexEqualWithoutValue(vertex, input.readVertex());
        }
    }

    @Test
    public void testWriteReadEdgesWithSingleFrequency() throws Exception {
        UnitTestBase.updateOptions(
            ComputerOptions.INPUT_EDGE_FREQ, "SINGLE"
        );
        ComputerContext context = ComputerContext.instance();
        GraphFactory graphFactory = context.graphFactory();

        Id longId = BytesId.of(100L);
        LongValue longValue = new LongValue(999L);
        Vertex vertex = graphFactory().createVertex(longId, longValue);
        vertex.addEdge(graphFactory.createEdge(BytesId.of(2L)));
        vertex.addEdge(graphFactory.createEdge("knows", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", "1111",
                                               BytesId.of(4L)));
        vertex.addEdge(graphFactory.createEdge("watch", "2222",
                                               BytesId.of(4L)));

        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeEdges(vertex);
            bytes = bao.toByteArray();
            bytes = reweaveBytes(bytes);
        }

        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
            assertEdgesEqual(vertex, input.readEdges(), EdgeFrequency.SINGLE);
        }
    }

    @Test
    public void testWriteReadEdgesWithSinglePerLabelFrequency()
           throws Exception {
        UnitTestBase.updateOptions(
            ComputerOptions.INPUT_EDGE_FREQ, "SINGLE_PER_LABEL"
        );
        ComputerContext context = ComputerContext.instance();
        GraphFactory graphFactory = context.graphFactory();

        Id longId = BytesId.of(100L);
        LongValue longValue = new LongValue(999L);
        Vertex vertex = graphFactory().createVertex(longId, longValue);
        vertex.addEdge(graphFactory.createEdge(BytesId.of(2L)));
        vertex.addEdge(graphFactory.createEdge("knows", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", "1111",
                                               BytesId.of(4L)));
        vertex.addEdge(graphFactory.createEdge("watch", "2222",
                                               BytesId.of(4L)));

        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeEdges(vertex);
            bytes = bao.toByteArray();
            bytes = reweaveBytes(bytes);
        }

        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
            assertEdgesEqual(vertex, input.readEdges(),
                             EdgeFrequency.SINGLE_PER_LABEL);
        }
    }

    @Test
    public void testWriteReadEdgesWithMultipleFrequency() throws Exception {
        UnitTestBase.updateOptions(
            ComputerOptions.INPUT_EDGE_FREQ, "MULTIPLE"
        );
        ComputerContext context = ComputerContext.instance();
        GraphFactory graphFactory = context.graphFactory();

        Id longId = BytesId.of(100L);
        LongValue longValue = new LongValue(999L);
        Vertex vertex = graphFactory().createVertex(longId, longValue);
        vertex.addEdge(graphFactory.createEdge(BytesId.of(2L)));
        vertex.addEdge(graphFactory.createEdge("knows", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", BytesId.of(3L)));
        vertex.addEdge(graphFactory.createEdge("watch", "1111",
                                               BytesId.of(4L)));
        vertex.addEdge(graphFactory.createEdge("watch", "2222",
                                               BytesId.of(4L)));

        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeEdges(vertex);
            bytes = bao.toByteArray();
            bytes = reweaveBytes(bytes);
        }

        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
            assertEdgesEqual(vertex, input.readEdges(), EdgeFrequency.MULTIPLE);
        }
    }

    @Test
    public void testWriteReadMessage() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_MESSAGE_CLASS, DoubleValue.class.getName()
        );

        Id id = BytesId.of(999L);
        Value value = new DoubleValue(0.85D);
        byte[] bytes;
        try (BytesOutput bao = IOFactory.createBytesOutput(
                               Constants.SMALL_BUF_SIZE)) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeMessage(id, value);
            bytes = bao.toByteArray();
            System.out.println(Arrays.toString(bytes));
        }

        try (BytesInput bai = IOFactory.createBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
            Assert.assertEquals(Pair.of(id, value), input.readMessage());
        }
    }

    private static byte[] reweaveBytes(byte[] oldBytes) throws IOException {
        BytesOutput bytesOutput = IOFactory.createBytesOutput(oldBytes.length);
        BytesInput bytesInput = IOFactory.createBytesInput(oldBytes);

        // key length
        int keyLength = bytesInput.readFixedInt();
        bytesOutput.writeFixedInt(keyLength);
        // key
        for (int i = 0; i < keyLength; i++) {
            bytesOutput.writeByte(bytesInput.readByte());
        }
        // total sub-entry length
        bytesOutput.writeFixedInt(bytesInput.readFixedInt());
        // sub-entry count
        int subEntryCount = bytesInput.readFixedInt();
        bytesOutput.writeFixedInt(subEntryCount);
        // Only write sub-entry key and value, doesn't write length
        for (int i = 0; i < subEntryCount; i++) {
            // sub-entry key length
            int subEntryKeyLength = bytesInput.readFixedInt();
            while (subEntryKeyLength > 0) {
                bytesOutput.writeByte(bytesInput.readByte());
                subEntryKeyLength--;
            }
            // sub-entry value length
            int subEntryValueLength = bytesInput.readFixedInt();
            while (subEntryValueLength > 0) {
                bytesOutput.writeByte(bytesInput.readByte());
                subEntryValueLength--;
            }
        }
        return bytesOutput.toByteArray();
    }

    /*
     * NOTE: this method will modify internal structure, please make sure that
     * will not rely on the original object structure after this call.
     */
    private static void assertVertexEqualWithoutValue(Vertex expect,
                                                      Vertex actual) {
        expect.value(null);
        actual.value(null);
        Assert.assertEquals(expect, actual);
    }

    /*
     * NOTE: this method will modify internal structure, please make sure that
     * will not rely on the original object structure after this call.
     */
    private static void assertEdgesEqual(Vertex expect, Vertex actual,
                                         EdgeFrequency frequency) {
        expect.value(null);
        actual.value(null);
        if (frequency == EdgeFrequency.SINGLE) {
            // Only compare targetId
            expect.edges().forEach(edge -> {
                edge.label(null);
                edge.name(null);
            });
            actual.edges().forEach(edge -> {
                edge.label(null);
                edge.name(null);
            });
        } else if (frequency == EdgeFrequency.SINGLE_PER_LABEL) {
            // Compare label and targetId
            expect.edges().forEach(edge -> {
                edge.name(null);
            });
            actual.edges().forEach(edge -> {
                edge.name(null);
            });
        } else {
            assert frequency == EdgeFrequency.MULTIPLE;
            // Compare label, name and targetId
        }
        Assert.assertEquals(expect, actual);
    }
}
