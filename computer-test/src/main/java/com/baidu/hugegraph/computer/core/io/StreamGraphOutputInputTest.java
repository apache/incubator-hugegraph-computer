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
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.edge.Edges;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.id.IdType;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.id.Utf8Id;
import com.baidu.hugegraph.computer.core.graph.properties.Properties;
import com.baidu.hugegraph.computer.core.graph.value.IdValueList;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.Lists;

public class StreamGraphOutputInputTest extends UnitTestBase {

    @Test
    public void testWriteReadVertex() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );

        LongId longId = new LongId(100L);
        LongValue longValue = new LongValue(999L);
        Vertex vertex1 = graphFactory().createVertex(longId, longValue);
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeVertex(vertex1);
            bytes = bao.toByteArray();
        }

        Vertex vertex2;
        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            vertex2 = input.readVertex();
        }
        Assert.assertEquals(vertex1, vertex2);
    }

    @Test
    public void testWriteReadEdges() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );
        GraphFactory factory = graphFactory();
        Edges edges1 = factory.createEdges(3);
        edges1.add(factory.createEdge(new LongId(100), new LongValue(1)));
        edges1.add(factory.createEdge(new LongId(200), new LongValue(2)));
        edges1.add(factory.createEdge(new LongId(300), new LongValue(-1)));

        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeEdges(edges1);
            bytes = bao.toByteArray();
        }

        Edges edges2;
        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            edges2 = input.readEdges();
        }
        Assert.assertEquals(edges1, edges2);
    }

    @Test
    public void testWriteReadEmptyEdges() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );

        Edges edges1 = graphFactory().createEdges(0);
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeEdges(edges1);
            bytes = bao.toByteArray();
        }

        Edges edges2;
        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            edges2 = input.readEdges();
        }
        Assert.assertEquals(edges1, edges2);
    }

    @Test
    public void testWriteReadProperties() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );

        /*
         * Config is global singleton instance, so the ValueType should be same
         * in one Properties, it seems unreasonable
         */
        Properties properties1 = graphFactory().createProperties();
        properties1.put("age", new LongValue(18L));
        properties1.put("salary", new LongValue(20000L));

        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeProperties(properties1);
            bytes = bao.toByteArray();
        }

        Properties properties2;
        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            properties2 = input.readProperties();
        }
        Assert.assertEquals(properties1, properties2);

        // Let ValueType as ID_VALUE
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_TYPE, "ID_VALUE",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );

        properties1 = graphFactory().createProperties();
        properties1.put("name", new Utf8Id("marko").idValue());
        properties1.put("city", new Utf8Id("Beijing").idValue());

        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeProperties(properties1);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            properties2 = input.readProperties();
        }
        Assert.assertEquals(properties1, properties2);
    }

    @Test
    public void testWriteReadId() throws IOException {
        LongId longId1 = new LongId(100L);
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeId(longId1);
            bytes = bao.toByteArray();
        }

        byte[] expect = new byte[]{IdType.LONG.code(), 100};
        Assert.assertArrayEquals(expect, bytes);

        Id longId2 = new LongId();
        Assert.assertEquals(0L, longId2.asLong());
        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            longId2 = input.readId();
        }
        Assert.assertEquals(100L, longId2.asLong());
    }

    @Test
    public void testWriteReadValue() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );

        LongValue longValue1 = new LongValue(100L);
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeValue(longValue1);
            bytes = bao.toByteArray();
        }

        byte[] expect = new byte[]{100};
        Assert.assertArrayEquals(expect, bytes);

        LongValue longValue2;
        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            longValue2 = (LongValue) input.readValue();
        }
        Assert.assertEquals(100L, longValue2.value());

        // Test IdValueList
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_TYPE, "ID_VALUE_LIST",
            ComputerOptions.VALUE_NAME, "value",
            ComputerOptions.EDGES_NAME, "value"
        );

        LongId longId1 = new LongId(100L);
        LongId longId2 = new LongId(200L);
        IdValueList idValueList1 = new IdValueList();
        idValueList1.add(longId1.idValue());
        idValueList1.add(longId2.idValue());
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeValue(idValueList1);
            bytes = bao.toByteArray();
        }

        expect = new byte[]{2, 2, 1, 100, 3, 1, -127, 72};
        Assert.assertArrayEquals(expect, bytes);

        IdValueList idValueList2;
        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            idValueList2 = (IdValueList) input.readValue();
        }
        Assert.assertTrue(ListUtils.isEqualList(
                          Lists.newArrayList(longId1.idValue(),
                                             longId2.idValue()),
                          idValueList2.values()));
    }

    @Test
    public void testReadWriteFullInt() throws IOException {
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeFullInt(Integer.MIN_VALUE);
            output.writeFullInt(Integer.MAX_VALUE);
            output.writeFullInt(0);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            Assert.assertEquals(Integer.MIN_VALUE, input.readFullInt());
            Assert.assertEquals(Integer.MAX_VALUE, input.readFullInt());
            Assert.assertEquals(0, input.readFullInt());
        }
    }

    @Test
    public void testReadWriteFullLong() throws IOException {
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeFullLong(Long.MIN_VALUE);
            output.writeFullLong(Long.MAX_VALUE);
            output.writeFullLong(0L);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            Assert.assertEquals(Long.MIN_VALUE, input.readFullLong());
            Assert.assertEquals(Long.MAX_VALUE, input.readFullLong());
            Assert.assertEquals(0L, input.readFullLong());
        }
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
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeVInt(value);
            Assert.assertArrayEquals(bytes, bao.toByteArray());
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            int readValue = input.readVInt();
            Assert.assertEquals(value, readValue);
        }
    }

    public static void testBytesStreamWriteReadVLong(byte[] bytes, long value)
                                                     throws IOException {
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeVLong(value);
            Assert.assertArrayEquals(bytes, bao.toByteArray());
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            long readValue = input.readVLong();
            Assert.assertEquals(value, readValue);
        }
    }

    public static void testBytesStreamWriteReadString(byte[] bytes,
                                                      String value)
                                                      throws IOException {
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeString(value);
            output.close();
            Assert.assertArrayEquals(bytes, bao.toByteArray());
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            String readValue = input.readString();
            Assert.assertEquals(value, readValue);
        }
    }

    @Test
    public void testPosition() throws IOException {
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            Assert.assertEquals(0L, output.position());
            output.writeFullLong(Long.MAX_VALUE);
            Assert.assertEquals(8L, output.position());
            output.close();
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            Assert.assertEquals(0L, input.position());
            Assert.assertEquals(Long.MAX_VALUE, input.readFullLong());
            Assert.assertEquals(8L, input.position());
        }
    }

    @Test
    public void testSeek() throws IOException {
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeFullLong(Long.MAX_VALUE);
            output.seek(0L);
            output.writeFullLong(Long.MIN_VALUE);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            Assert.assertEquals(Long.MIN_VALUE, input.readFullLong());
            input.seek(0L);
            Assert.assertEquals(Long.MIN_VALUE, input.readFullLong());
        }
    }

    @Test
    public void testSkip() throws IOException {
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeFullLong(Long.MAX_VALUE);
            output.skip(4L);
            output.writeFullLong(Long.MIN_VALUE);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            Assert.assertEquals(Long.MAX_VALUE, input.readFullLong());
            input.skip(4L);
            Assert.assertEquals(Long.MIN_VALUE, input.readFullLong());
        }
    }

    @Test
    public void testUInt8() throws IOException {
        byte[] bytes;
        int value1 = 0;
        int value2 = 255;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newOptimizedStreamGraphOutput(bao)) {
            output.writeUInt8(value1);
            output.writeUInt8(value2);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newOptimizedStreamGraphInput(bai)) {
            Assert.assertEquals(value1, input.readUInt8());
            Assert.assertEquals(value2, input.readUInt8());
        }
    }

    @Test
    public void testUInt16() throws IOException {
        byte[] bytes;
        int value1 = 0;
        int value2 = 65535;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newStreamGraphOutput(bao)) {
            output.writeUInt16(value1);
            output.writeUInt16(value2);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newStreamGraphInput(bai)) {
            Assert.assertEquals(value1, input.readUInt16());
            Assert.assertEquals(value2, input.readUInt16());
        }
    }

    @Test
    public void testUInt32() throws IOException {
        byte[] bytes;
        long value1 = 0L;
        long value2 = Integer.MAX_VALUE + 0L;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newStreamGraphOutput(bao)) {
            output.writeUInt32(value1);
            output.writeUInt32(value2);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newStreamGraphInput(bai)) {
            Assert.assertEquals(value1, input.readUInt32());
            Assert.assertEquals(value2, input.readUInt32());
        }
    }

    @Test
    public void testShort() throws IOException {
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newStreamGraphOutput(bao)) {
            output.writeShort(Short.MAX_VALUE);
            output.writeShort(Short.MIN_VALUE);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newStreamGraphInput(bai)) {
            Assert.assertEquals(Short.MAX_VALUE, input.readShort());
            Assert.assertEquals(Short.MIN_VALUE, input.readShort());
        }
    }

    @Test
    public void testInt() throws IOException {
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newStreamGraphOutput(bao)) {
            output.writeInt(Integer.MAX_VALUE);
            output.writeInt(Integer.MIN_VALUE);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newStreamGraphInput(bai)) {
            Assert.assertEquals(Integer.MAX_VALUE, input.readInt());
            Assert.assertEquals(Integer.MIN_VALUE, input.readInt());
        }
    }

    @Test
    public void testLong() throws IOException {
        byte[] bytes;
        try (UnsafeByteArrayOutput bao = new UnsafeByteArrayOutput();
             StreamGraphOutput output = newStreamGraphOutput(bao)) {
            output.writeLong(Long.MAX_VALUE);
            output.writeLong(Long.MIN_VALUE);
            bytes = bao.toByteArray();
        }

        try (UnsafeByteArrayInput bai = new UnsafeByteArrayInput(bytes);
             StreamGraphInput input = newStreamGraphInput(bai)) {
            Assert.assertEquals(Long.MAX_VALUE, input.readLong());
            Assert.assertEquals(Long.MIN_VALUE, input.readLong());
        }
    }
}
