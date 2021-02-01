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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.edge.DefaultEdge;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValue;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.FloatValue;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.graph.value.IdValueList;
import com.baidu.hugegraph.computer.core.graph.value.IdValueListList;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.vertex.DefaultVertex;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

// TODO: Let it pass
public class CsvStructGraphOutputTest {

    @Test
    public void testWriteReadVertexOnlyIdAndValue() throws IOException {
        ComputerContext.parseOptions(
        ComputerOptions.VALUE_TYPE.name(), "LONG",
        ComputerOptions.OUTPUT_VERTEX_ADJACENT_EDGES.name(), "false",
        ComputerOptions.OUTPUT_VERTEX_PROPERTIES.name(), "false",
        ComputerOptions.OUTPUT_EDGE_PROPERTIES.name(), "false");

        LongId longId = new LongId(100L);
        IdValue idValue = new LongId(999L).idValue();
        Vertex<IdValue, IdValue> vertex = new DefaultVertex<>(longId, idValue);

        String fileName = "output.csv";
        File file = new File(fileName);
        try (FileOutputStream fos = new FileOutputStream(file);
             DataOutputStream dos = new DataOutputStream(fos)) {
            StructGraphOutput output = (StructGraphOutput)
                                       GraphOutputFactory.create(
                                       OutputFormat.CSV, dos);
            output.writeVertex(vertex);

            String text = FileUtils.readFileToString(file);
            Assert.assertEquals("100,999" + System.lineSeparator(), text);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testWriteReadVertexWithOutEdges() throws IOException {
        ComputerContext.parseOptions(
        ComputerOptions.VALUE_TYPE.name(), "LONG",
        ComputerOptions.OUTPUT_VERTEX_ADJACENT_EDGES.name(), "true",
        ComputerOptions.OUTPUT_VERTEX_PROPERTIES.name(), "false",
        ComputerOptions.OUTPUT_EDGE_PROPERTIES.name(), "false");

        LongId longId = new LongId(100L);
        IdValueList idValueList = new IdValueList();
        idValueList.add(new LongId(998L).idValue());
        idValueList.add(new LongId(999L).idValue());
        Vertex<IdValueList, LongValue> vertex = new DefaultVertex<>(
                                                longId, idValueList);
        vertex.addEdge(new DefaultEdge<>(new LongId(200), new LongValue(1)));
        vertex.addEdge(new DefaultEdge<>(new LongId(300), new LongValue(-1)));

        String fileName = "output2.csv";
        File file = new File(fileName);
        try (FileOutputStream fos = new FileOutputStream(file);
             DataOutputStream dos = new DataOutputStream(fos)) {
            StructGraphOutput output = (StructGraphOutput)
                                       GraphOutputFactory.create(
                                       OutputFormat.CSV, dos);
            output.writeVertex(vertex);

            String json = FileUtils.readFileToString(file);
            Assert.assertEquals("{\"id\":100,\"page_rank\":[998,999]," +
                                "\"adjacent_edges\":[{\"target_id\":200," +
                                "\"value\":1},{\"target_id\":300," +
                                "\"value\":-1}]}", json);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testWriteReadVertexWithProperties() throws IOException {
        ComputerContext.parseOptions(
        ComputerOptions.VALUE_TYPE.name(), "LONG",
        ComputerOptions.OUTPUT_VERTEX_ADJACENT_EDGES.name(), "false",
        ComputerOptions.OUTPUT_VERTEX_PROPERTIES.name(), "true",
        ComputerOptions.OUTPUT_EDGE_PROPERTIES.name(), "false");

        LongId longId = new LongId(100L);
        IdValueListList idValueListList = new IdValueListList();
        IdValueList idValueList1 = new IdValueList();
        idValueList1.add(new LongId(66L).idValue());
        IdValueList idValueList2 = new IdValueList();
        idValueList2.add(new LongId(998L).idValue());
        idValueList2.add(new LongId(999L).idValue());
        idValueListList.add(idValueList1);
        idValueListList.add(idValueList2);

        Vertex<IdValueListList, LongValue> vertex = new DefaultVertex<>(
                                                    longId, idValueListList);
        vertex.properties().put("boolean", new BooleanValue(true));
        vertex.properties().put("byte", new IntValue(127));
        vertex.properties().put("short", new IntValue(16383));
        vertex.properties().put("int", new IntValue(1000000));
        vertex.properties().put("long", new LongValue(10000000000L));
        vertex.properties().put("float", new FloatValue(0.1F));
        vertex.properties().put("double", new DoubleValue(-0.01D));
        vertex.properties().put("idvalue", longId.idValue());

        String fileName = "output3.csv";
        File file = new File(fileName);
        try (FileOutputStream fos = new FileOutputStream(file);
             DataOutputStream dos = new DataOutputStream(fos)) {
            StructGraphOutput output = (StructGraphOutput)
                                       GraphOutputFactory.create(
                                       OutputFormat.CSV, dos);
            output.writeVertex(vertex);

            String json = FileUtils.readFileToString(file);
            Assert.assertEquals("{\"id\":100,\"page_rank\":[[66],[998,999]]," +
                                "\"properties\":{\"boolean\":true," +
                                "\"byte\":127,\"double\":-0.01," +
                                "\"short\":16383,\"idvalue\":100," +
                                "\"float\":0.1,\"int\":1000000," +
                                "\"long\":10000000000}}", json);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }
}
