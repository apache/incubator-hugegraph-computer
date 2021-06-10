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
import org.junit.Test;

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
import com.baidu.hugegraph.computer.suite.unit.UnitTestBase;
import com.baidu.hugegraph.testutil.Assert;

public class JsonStructGraphOutputTest extends UnitTestBase {

    @Test
    public void testWriteReadVertexOnlyIdAndValue() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_NAME, "rank",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES, "false",
            ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES, "false",
            ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES, "false"
        );
        ComputerContext context = context();
        GraphFactory factory = context.graphFactory();

        LongId longId = new LongId(100L);
        IdValue idValue = new LongId(999L).idValue();
        Vertex vertex = factory.createVertex(longId, idValue);

        String fileName = "output.json";
        File file = new File(fileName);
        try {
            BufferedFileOutput dos = new BufferedFileOutput(file);
            StructGraphOutput output = (StructGraphOutput)
                                       IOFactory.createGraphOutput(
                                       context, OutputFormat.JSON, dos);
            output.writeVertex(vertex);
            dos.close();

            String json = FileUtils.readFileToString(file);
            Assert.assertEquals("{\"id\":100,\"rank\":999}" +
                                System.lineSeparator(), json);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testWriteReadVertexWithEdges() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_NAME, "rank",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES, "true",
            ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES, "false",
            ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES, "false"
        );
        ComputerContext context = context();
        GraphFactory factory = context.graphFactory();

        LongId longId = new LongId(100L);
        IdValueList idValueList = new IdValueList();
        idValueList.add(new LongId(998L).idValue());
        idValueList.add(new LongId(999L).idValue());
        Vertex vertex = factory.createVertex(longId, idValueList);
        vertex.addEdge(factory.createEdge("knows", new LongId(200)));
        vertex.addEdge(factory.createEdge("watch", "1111", new LongId(300)));

        String fileName = "output.json";
        File file = new File(fileName);
        try {
            BufferedFileOutput dos = new BufferedFileOutput(file);
            StructGraphOutput output = (StructGraphOutput)
                                       IOFactory.createGraphOutput(
                                       context, OutputFormat.JSON, dos);
            output.writeVertex(vertex);
            dos.close();

            String json = FileUtils.readFileToString(file);
            Assert.assertEquals("{\"id\":100,\"rank\":[998,999]," +
                                "\"adjacent_edges\":[{\"target_id\":200," +
                                "\"label\":\"knows\",\"name\":\"\"}," +
                                "{\"target_id\":300,\"label\":\"watch\"," +
                                "\"name\":\"1111\"}]}" +
                                System.lineSeparator(), json);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test
    public void testWriteReadVertexWithProperties() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.VALUE_NAME, "rank",
            ComputerOptions.VALUE_TYPE, "LONG",
            ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES, "false",
            ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES, "true",
            ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES, "false"
        );
        ComputerContext context = context();
        GraphFactory factory = context.graphFactory();

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

        String fileName = "output.json";
        File file = new File(fileName);
        try {
            BufferedFileOutput dos = new BufferedFileOutput(file);
            StructGraphOutput output = (StructGraphOutput)
                                       IOFactory.createGraphOutput(
                                       context, OutputFormat.JSON, dos);
            output.writeVertex(vertex);
            dos.close();

            String json = FileUtils.readFileToString(file);
            Assert.assertEquals("{\"id\":100,\"rank\":[[66],[998,999]]," +
                                "\"properties\":{\"boolean\":true," +
                                "\"byte\":127,\"double\":-0.01," +
                                "\"short\":16383,\"idvalue\":100," +
                                "\"float\":0.1,\"int\":1000000," +
                                "\"long\":10000000000}}" +
                                System.lineSeparator(), json);
        } finally {
            FileUtils.deleteQuietly(file);
        }
    }
}
