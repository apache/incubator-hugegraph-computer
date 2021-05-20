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
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.IdValueList;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
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
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput()) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeVertex(vertex1);
            bytes = bao.toByteArray();
        }

        Vertex vertex2;
        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
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
        edges1.add(factory.createEdge(new LongId(100)));
        edges1.add(factory.createEdge(new LongId(200)));
        edges1.add(factory.createEdge(new LongId(300)));

        byte[] bytes;
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput()) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeEdges(edges1);
            bytes = bao.toByteArray();
        }

        Edges edges2;
        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
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
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput()) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeEdges(edges1);
            bytes = bao.toByteArray();
        }

        Edges edges2;
        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
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
        properties1.put("salary", new DoubleValue(20000.50D));

        byte[] bytes;
        try (UnsafeBytesOutput bao = new UnsafeBytesOutput()) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeProperties(properties1);
            bytes = bao.toByteArray();
        }

        Properties properties2;
        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
            properties2 = input.readProperties();
        }
        Assert.assertEquals(properties1, properties2);

        properties1 = graphFactory().createProperties();
        properties1.put("name", new Utf8Id("marko").idValue());
        properties1.put("age", new IntValue(18));

        try (UnsafeBytesOutput bao = new UnsafeBytesOutput()) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeProperties(properties1);
            bytes = bao.toByteArray();
        }

        try (UnsafeBytesInput bai = new UnsafeBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
            properties2 = input.readProperties();
        }
        Assert.assertEquals(properties1, properties2);
    }

    @Test
    public void testWriteReadId() throws IOException {
        LongId longId1 = new LongId(100L);
        byte[] bytes;
        try (UnsafeBytesOutput bao = new OptimizedUnsafeBytesOutput()) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeId(longId1);
            bytes = bao.toByteArray();
        }

        byte[] expect = new byte[]{IdType.LONG.code(), 100};
        Assert.assertArrayEquals(expect, bytes);

        Id longId2 = new LongId();
        Assert.assertEquals(0L, longId2.asLong());
        try (UnsafeBytesInput bai =
             new OptimizedUnsafeBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
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
        try (UnsafeBytesOutput bao = new OptimizedUnsafeBytesOutput()) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeValue(longValue1);
            bytes = bao.toByteArray();
        }

        byte[] expect = new byte[]{100};
        Assert.assertArrayEquals(expect, bytes);

        LongValue longValue2;
        try (UnsafeBytesInput bai = new OptimizedUnsafeBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
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
        try (UnsafeBytesOutput bao = new OptimizedUnsafeBytesOutput()) {
            StreamGraphOutput output = newStreamGraphOutput(bao);
            output.writeValue(idValueList1);
            bytes = bao.toByteArray();
        }

        expect = new byte[]{2, 2, 1, 100, 3, 1, -127, 72};
        Assert.assertArrayEquals(expect, bytes);

        IdValueList idValueList2;
        try (UnsafeBytesInput bai = new OptimizedUnsafeBytesInput(bytes)) {
            StreamGraphInput input = newStreamGraphInput(bai);
            idValueList2 = (IdValueList) input.readValue();
        }
        Assert.assertTrue(ListUtils.isEqualList(
                          Lists.newArrayList(longId1.idValue(),
                                             longId2.idValue()),
                          idValueList2.values()));
    }
}
