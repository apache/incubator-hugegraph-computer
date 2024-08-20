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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.graph.GraphFactory;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.core.graph.value.BooleanValue;
import org.apache.hugegraph.computer.core.graph.value.DoubleValue;
import org.apache.hugegraph.computer.core.graph.value.FloatValue;
import org.apache.hugegraph.computer.core.graph.value.IdList;
import org.apache.hugegraph.computer.core.graph.value.IdListList;
import org.apache.hugegraph.computer.core.graph.value.IntValue;
import org.apache.hugegraph.computer.core.graph.value.LongValue;
import org.apache.hugegraph.computer.core.graph.value.Value;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class JsonStructGraphOutputTest extends UnitTestBase {

    @Test
    public void testWriteReadVertexOnlyIdAndValue() throws IOException {
        UnitTestBase.updateOptions(
            ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES, "false",
            ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES, "false",
            ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES, "false",
            ComputerOptions.OUTPUT_RESULT_NAME, "rank"
        );
        ComputerContext context = context();
        GraphFactory factory = context.graphFactory();

        Id longId = BytesId.of(100L);
        Value value = BytesId.of(999L);
        Vertex vertex = factory.createVertex(longId, value);

        String fileName = "output.json";
        File file = new File(fileName);
        try {
            BufferedFileOutput dos = new BufferedFileOutput(file);
            StructGraphOutput output = (StructGraphOutput)
                                       IOFactory.createGraphOutput(
                                       context, OutputFormat.JSON, dos);
            output.writeVertex(vertex);
            dos.close();

            @SuppressWarnings("deprecation")
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
            ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES, "true",
            ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES, "false",
            ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES, "false",
            ComputerOptions.OUTPUT_RESULT_NAME, "rank"
        );
        ComputerContext context = context();
        GraphFactory factory = context.graphFactory();

        Id longId = BytesId.of(100L);
        IdList idList = new IdList();
        idList.add(BytesId.of(998L));
        idList.add(BytesId.of(999L));
        Vertex vertex = factory.createVertex(longId, idList);
        vertex.addEdge(factory.createEdge("knows", BytesId.of(200)));
        vertex.addEdge(factory.createEdge("watch", "1111", BytesId.of(300)));

        String fileName = "output.json";
        File file = new File(fileName);
        try {
            BufferedFileOutput dos = new BufferedFileOutput(file);
            StructGraphOutput output = (StructGraphOutput)
                                       IOFactory.createGraphOutput(
                                       context, OutputFormat.JSON, dos);
            output.writeVertex(vertex);
            dos.close();

            @SuppressWarnings("deprecation")
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
            ComputerOptions.OUTPUT_WITH_ADJACENT_EDGES, "false",
            ComputerOptions.OUTPUT_WITH_VERTEX_PROPERTIES, "true",
            ComputerOptions.OUTPUT_WITH_EDGE_PROPERTIES, "false",
            ComputerOptions.OUTPUT_RESULT_NAME, "rank"
        );
        ComputerContext context = context();
        GraphFactory factory = context.graphFactory();

        Id longId = BytesId.of(100L);
        IdListList idListList = new IdListList();
        IdList idList1 = new IdList();
        idList1.add(BytesId.of(66L));
        IdList idList2 = new IdList();
        idList2.add(BytesId.of(998L));
        idList2.add(BytesId.of(999L));
        idListList.add(idList1);
        idListList.add(idList2);

        Vertex vertex = factory.createVertex(longId, idListList);
        vertex.properties().put("boolean", new BooleanValue(true));
        vertex.properties().put("byte", new IntValue(127));
        vertex.properties().put("short", new IntValue(16383));
        vertex.properties().put("int", new IntValue(1000000));
        vertex.properties().put("long", new LongValue(10000000000L));
        vertex.properties().put("float", new FloatValue(0.1F));
        vertex.properties().put("double", new DoubleValue(-0.01D));
        vertex.properties().put("idvalue", longId);

        String fileName = "output.json";
        File file = new File(fileName);
        try {
            BufferedFileOutput dos = new BufferedFileOutput(file);
            StructGraphOutput output = (StructGraphOutput)
                                       IOFactory.createGraphOutput(
                                       context, OutputFormat.JSON, dos);
            output.writeVertex(vertex);
            dos.close();

            @SuppressWarnings("deprecation")
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
