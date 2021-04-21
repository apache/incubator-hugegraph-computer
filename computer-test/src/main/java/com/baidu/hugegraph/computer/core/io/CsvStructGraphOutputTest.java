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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.value.BooleanValue;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.FloatValue;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.graph.value.IdValueList;
import com.baidu.hugegraph.computer.core.graph.value.IdValueListList;
import com.baidu.hugegraph.computer.core.graph.value.IntValue;
import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;

public class CsvStructGraphOutputTest {

    @Test
    public void testWriteReadVertexOnlyIdAndValue() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_NAME, "page_rank",
            ComputerOptions.VALUE_NAME, "rank",
            ComputerOptions.EDGES_NAME, "value",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES, "false",
            ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES, "false",
            ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES, "false"
        );
        GraphFactory factory = ComputerContext.instance().graphFactory();

        LongId longId = new LongId(100L);
        IdValue idValue = new LongId(999L).idValue();
        Vertex vertex = factory.createVertex(longId, idValue);

        String fileName = "output.csv";
        File file = new File(fileName);
        try {
            BufferedFileOutput dos = new BufferedFileOutput(file);
            StructGraphOutput output = (StructGraphOutput)
                                       GraphOutputFactory.create(
                                       OutputFormat.CSV, dos);
            output.writeVertex(vertex);
            output.close();

            String text = FileUtils.readFileToString(file);
            Assert.assertEquals("100,999" + System.lineSeparator(), text);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testWriteReadVertexWithEdges() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_NAME, "page_rank",
            ComputerOptions.VALUE_NAME, "rank",
            ComputerOptions.EDGES_NAME, "value",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES, "true",
            ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES, "false",
            ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES, "false"
        );
        GraphFactory factory = ComputerContext.instance().graphFactory();

        LongId longId = new LongId(100L);
        IdValueList idValueList = new IdValueList();
        idValueList.add(new LongId(998L).idValue());
        idValueList.add(new LongId(999L).idValue());
        Vertex vertex = factory.createVertex(longId, idValueList);
        vertex.addEdge(factory.createEdge(new LongId(200), new LongValue(1)));
        vertex.addEdge(factory.createEdge(new LongId(300), new LongValue(-1)));

        String fileName = "output2.csv";
        File file = new File(fileName);
        try {
            BufferedFileOutput dos = new BufferedFileOutput(file);
            StructGraphOutput output = (StructGraphOutput)
                                       GraphOutputFactory.create(
                                       OutputFormat.CSV, dos);
            output.writeVertex(vertex);
            output.close();

            String json = FileUtils.readFileToString(file);
            Assert.assertEquals("100,[998,999],[{200,1},{300,-1}]" +
                                System.lineSeparator(), json);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testWriteReadVertexWithProperties() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.ALGORITHM_NAME, "page_rank",
            ComputerOptions.VALUE_NAME, "rank",
            ComputerOptions.EDGES_NAME, "value",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES, "false",
            ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES, "true",
            ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES, "false"
        );
        GraphFactory factory = ComputerContext.instance().graphFactory();

        LongId longId = new LongId(100L);
        IdValueListList idValueListList = new IdValueListList();
        IdValueList idValueList1 = new IdValueList();
        idValueList1.add(new LongId(66L).idValue());
        IdValueList idValueList2 = new IdValueList();
        idValueList2.add(new LongId(998L).idValue());
        idValueList2.add(new LongId(999L).idValue());
        idValueListList.add(idValueList1);
        idValueListList.add(idValueList2);

        Vertex vertex = factory.createVertex(longId, idValueListList);
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
        try {
            BufferedFileOutput dos = new BufferedFileOutput(file);
            StructGraphOutput output = (StructGraphOutput)
                                       GraphOutputFactory.create(
                                       OutputFormat.CSV, dos);
            output.writeVertex(vertex);
            output.close();

            String json = FileUtils.readFileToString(file);
            Assert.assertEquals("100,[[66],[998,999]],{true,127,-0.01,16383," +
                                "100,0.1,1000000,10000000000}" +
                                System.lineSeparator(), json);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }
}
