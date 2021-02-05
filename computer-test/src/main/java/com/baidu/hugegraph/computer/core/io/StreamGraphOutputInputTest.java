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

package com.baidu.hugegraph.computer.core.io;

import java.io.IOException;

import org.apache.commons.collections.ListUtils;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdType;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.id.Utf8Id;
import com.baidu.hugegraph.computer.core.graph.properties.DefaultProperties;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.IdValueList;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.Lists;

public class StreamGraphOutputInputTest {

    @Test
    public void testWriteReadVertex() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_NAME, "test",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );
        GraphFactory factory = ComputerContext.instance().graphFactory();

        LongId longId = new LongId(100L);
        LongValue longValue = new LongValue(999L);
        Vertex vertex1 = factory.createVertex(longId, longValue);
        byte[] bytes;
        try (UnsafeByteArrayOutput ubao = new UnsafeByteArrayOutput()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(ubao);
            output.writeVertex(vertex1);
            bytes = ubao.toByteArray();
        }

        Vertex vertex2;
        try (UnsafeByteArrayInput ubai = new UnsafeByteArrayInput(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(ubai);
            vertex2 = input.readVertex();
        }
        Assert.assertEquals(vertex1, vertex2);
    }

    @Test
    public void testWriteReadEdges() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_NAME, "test",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );
        GraphFactory factory = ComputerContext.instance().graphFactory();

        Edges edges1 = factory.createEdges(3);
        edges1.add(factory.createEdge(new LongId(100), new LongValue(1)));
        edges1.add(factory.createEdge(new LongId(200), new LongValue(2)));
        edges1.add(factory.createEdge(new LongId(300), new LongValue(-1)));

        byte[] bytes;
        try (UnsafeByteArrayOutput ubao = new UnsafeByteArrayOutput()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(ubao);
            output.writeEdges(edges1);
            bytes = ubao.toByteArray();
        }

        Edges edges2;
        try (UnsafeByteArrayInput ubai = new UnsafeByteArrayInput(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(ubai);
            edges2 = input.readEdges();
        }
        Assert.assertEquals(edges1, edges2);
    }

    @Test
    public void testWriteReadProperties() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_NAME, "test",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );

        /*
         * Config is global singleton instance, so the ValueType should be same
         * in one Properties, it seems unreasonable
         */
        Properties properties1 = new DefaultProperties();
        properties1.put("age", new LongValue(18L));
        properties1.put("salary", new LongValue(20000L));

        byte[] bytes;
        try (UnsafeByteArrayOutput ubao = new UnsafeByteArrayOutput()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(ubao);
            output.writeProperties(properties1);
            bytes = ubao.toByteArray();
        }

        Properties properties2;
        try (UnsafeByteArrayInput ubai = new UnsafeByteArrayInput(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(ubai);
            properties2 = input.readProperties();
        }
        Assert.assertEquals(properties1, properties2);

        // Let ValueType as ID_VALUE
        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_NAME, "test",
            ComputerOptions.VALUE_TYPE, "ID_VALUE",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );

        properties1 = new DefaultProperties();
        properties1.put("name", new Utf8Id("marko").idValue());
        properties1.put("city", new Utf8Id("Beijing").idValue());

        try (UnsafeByteArrayOutput ubao = new UnsafeByteArrayOutput()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(ubao);
            output.writeProperties(properties1);
            bytes = ubao.toByteArray();
        }

        try (UnsafeByteArrayInput ubai = new UnsafeByteArrayInput(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(ubai);
            properties2 = input.readProperties();
        }
        Assert.assertEquals(properties1, properties2);
    }

    @Test
    public void testWriteReadId() throws IOException {
        LongId longId1 = new LongId(100L);
        byte[] bytes;
        try (UnsafeByteArrayOutput ubao = new UnsafeByteArrayOutput()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(ubao);
            output.writeId(longId1);
            bytes = ubao.toByteArray();
        }

        byte[] expect = new byte[]{IdType.LONG.code(), 100};
        Assert.assertArrayEquals(expect, bytes);

        Id longId2 = new LongId();
        Assert.assertEquals(0L, longId2.asLong());
        try (UnsafeByteArrayInput ubai = new UnsafeByteArrayInput(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(ubai);
            longId2 = input.readId();
        }
        Assert.assertEquals(100L, longId2.asLong());
    }

    @Test
    public void testWriteReadValue() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_NAME, "test",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );

        LongValue longValue1 = new LongValue(100L);
        byte[] bytes;
        try (UnsafeByteArrayOutput ubao = new UnsafeByteArrayOutput()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(ubao);
            output.writeValue(longValue1);
            bytes = ubao.toByteArray();
        }

        byte[] expect = new byte[]{100};
        Assert.assertArrayEquals(expect, bytes);

        LongValue longValue2;
        try (UnsafeByteArrayInput ubai = new UnsafeByteArrayInput(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(ubai);
            longValue2 = (LongValue) input.readValue();
        }
        Assert.assertEquals(100L, longValue2.value());

        // Test IdValueList
        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_NAME, "test",
            ComputerOptions.VALUE_TYPE, "ID_VALUE_LIST",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );

        LongId longId1 = new LongId(100L);
        LongId longId2 = new LongId(200L);
        IdValueList idValueList1 = new IdValueList();
        idValueList1.add(longId1.idValue());
        idValueList1.add(longId2.idValue());
        try (UnsafeByteArrayOutput ubao = new UnsafeByteArrayOutput()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(ubao);
            output.writeValue(idValueList1);
            bytes = ubao.toByteArray();
        }

        expect = new byte[]{2, 2, 1, 100, 3, 1, -127, 72};
        Assert.assertArrayEquals(expect, bytes);

        IdValueList idValueList2;
        try (UnsafeByteArrayInput ubai = new UnsafeByteArrayInput(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(ubai);
            idValueList2 = (IdValueList) input.readValue();
        }
        Assert.assertTrue(ListUtils.isEqualList(
                          Lists.newArrayList(longId1.idValue(),
                                             longId2.idValue()),
                          idValueList2.values()));
    }

    @Test
    public void testWriteReadVInt() throws IOException {
        testBytesStreamWriteReadVInt(new byte[]{0}, 0);
        testBytesStreamWriteReadVInt(new byte[]{1}, 1);
        testBytesStreamWriteReadVInt(new byte[]{(byte) 0x7f}, 127);
        testBytesStreamWriteReadVInt(new byte[]{(byte) 0x81, 0}, 128);
        testBytesStreamWriteReadVInt(new byte[]{(byte) 0xff, (byte) 0x7f},
                                     16383);
        testBytesStreamWriteReadVInt(new byte[]{(byte) 0x81, (byte) 0x80, 0},
                                     16384);
        testBytesStreamWriteReadVInt(new byte[]{(byte) 0x81, (byte) 0x80, 1},
                                     16385);
        testBytesStreamWriteReadVInt(new byte[]{-113, -1, -1, -1, 127}, -1);
        testBytesStreamWriteReadVInt(new byte[]{-121, -1, -1, -1, 127},
                                     Integer.MAX_VALUE);
        testBytesStreamWriteReadVInt(new byte[]{-120, -128, -128, -128, 0},
                                     Integer.MIN_VALUE);
    }

    @Test
    public void testWriteReadVLong() throws IOException {
        testBytesStreamWriteReadVLong(new byte[]{0}, 0L);
        testBytesStreamWriteReadVLong(new byte[]{1}, 1L);
        testBytesStreamWriteReadVLong(new byte[]{(byte) 0x7f}, 127L);
        testBytesStreamWriteReadVLong(new byte[]{(byte) 0x81, 0}, 128L);
        testBytesStreamWriteReadVLong(new byte[]{(byte) 0xff, (byte) 0x7f},
                                      16383L);
        testBytesStreamWriteReadVLong(new byte[]{(byte) 0x81, (byte) 0x80, 0},
                                      16384L);
        testBytesStreamWriteReadVLong(new byte[]{(byte) 0x81, (byte) 0x80, 1},
                                      16385L);
        testBytesStreamWriteReadVLong(new byte[]{-127, -1, -1, -1, -1,
                                                 -1, -1, -1, -1, 127}, -1L);
        testBytesStreamWriteReadVLong(new byte[]{-121, -1, -1, -1, 127},
                                      Integer.MAX_VALUE);
        testBytesStreamWriteReadVLong(new byte[]{-127, -1, -1, -1, -1,
                                                 -8, -128, -128, -128, 0},
                                      Integer.MIN_VALUE);
        testBytesStreamWriteReadVLong(new byte[]{-1, -1, -1, -1, -1,
                                                 -1, -1, -1, 127},
                                      Long.MAX_VALUE);
        testBytesStreamWriteReadVLong(new byte[]{-127, -128, -128, -128, -128,
                                                 -128, -128, -128, -128, 0},
                                      Long.MIN_VALUE);
    }

    @Test
    public void testWriteReadString() throws IOException {
        testBytesStreamWriteReadString(new byte[]{0}, "");
        testBytesStreamWriteReadString(new byte[]{1, 49}, "1");
        testBytesStreamWriteReadString(new byte[]{3, 55, 56, 57}, "789");
        testBytesStreamWriteReadString(new byte[]{5, 65, 66, 67, 68, 69},
                                       "ABCDE");
    }

    public static void testBytesStreamWriteReadVInt(byte[] bytes, int value)
                                                    throws IOException {
        try (UnsafeByteArrayOutput ubao = new UnsafeByteArrayOutput(5)) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(ubao);
            output.writeVInt(value);
            Assert.assertArrayEquals(bytes, ubao.toByteArray());
        }

        try (UnsafeByteArrayInput ubai = new UnsafeByteArrayInput(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(ubai);
            int readValue = input.readVInt();
            Assert.assertEquals(value, readValue);
        }
    }

    public static void testBytesStreamWriteReadVLong(byte[] bytes, long value)
                                                     throws IOException {
        try (UnsafeByteArrayOutput ubao = new UnsafeByteArrayOutput(9)) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(ubao);
            output.writeVLong(value);
            Assert.assertArrayEquals(bytes, ubao.toByteArray());
        }

        try (UnsafeByteArrayInput ubai = new UnsafeByteArrayInput(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(ubai);
            long readValue = input.readVLong();
            Assert.assertEquals(value, readValue);
        }
    }

    public static void testBytesStreamWriteReadString(byte[] bytes,
                                                      String value)
                                                      throws IOException {
        try (UnsafeByteArrayOutput ubao = new UnsafeByteArrayOutput()) {
            StreamGraphOutput output = new OptimizedStreamGraphOutput(ubao);
            output.writeString(value);
            Assert.assertArrayEquals(bytes, ubao.toByteArray());
        }

        try (UnsafeByteArrayInput ubai = new UnsafeByteArrayInput(bytes)) {
            StreamGraphInput input = new OptimizedStreamGraphInput(ubai);
            String readValue = input.readString();
            Assert.assertEquals(value, readValue);
        }
    }
}
