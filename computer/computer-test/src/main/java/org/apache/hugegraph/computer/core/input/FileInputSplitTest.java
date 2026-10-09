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

package org.apache.hugegraph.computer.core.input;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.input.loader.FileEdgeFetcher;
import org.apache.hugegraph.computer.core.input.loader.FileInputSplit;
import org.apache.hugegraph.computer.core.input.loader.FileVertxFetcher;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.loader.constant.ElemType;
import org.apache.hugegraph.loader.mapping.InputStruct;
import org.apache.hugegraph.loader.mapping.LoadMapping;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class FileInputSplitTest extends UnitTestBase {

    @Rule
    public TemporaryFolder files = new TemporaryFolder();

    @Test
    public void testOfflineVertexAndEdgeInput() throws Exception {
        String mappingJson = Files.readString(Paths.get(
                             "src/main/resources/hdfs_input_test/struct.json"))
                                  .replace("\"HDFS\"", "\"FILE\"")
                                  .replace("\"core_site_path\": " +
                                           "\"src/main/resources/hdfs_input_test/" +
                                           "core-site.xml\",", "");
        File mappingFile = this.files.newFile("struct.json");
        Files.writeString(mappingFile.toPath(), mappingJson, StandardCharsets.UTF_8);
        LoadMapping mapping = LoadMapping.of(mappingFile.getAbsolutePath());
        Config config = Mockito.mock(Config.class);
        Mockito.when(config.get(ComputerOptions.INPUT_LOADER_SCHEMA_PATH))
               .thenReturn("src/main/resources/hdfs_input_test/schema.json");

        File users = this.files.newFile("users.csv");
        Files.writeString(users.toPath(), "userId\n1\n2\n", StandardCharsets.UTF_8);
        FileVertxFetcher vertexFetcher = new FileVertxFetcher(config);
        try {
            vertexFetcher.prepareLoadInputSplit(new FileInputSplit(
                    ElemType.VERTEX, mapping.structs().get(0), users.getAbsolutePath()));
            Vertex first = vertexFetcher.next();
            Vertex second = vertexFetcher.next();
            Assert.assertEquals("user", first.label());
            Assert.assertEquals(1, first.properties().get("id"));
            Assert.assertEquals(2, second.properties().get("id"));
            Assert.assertFalse(vertexFetcher.hasNext());
        } finally {
            vertexFetcher.close();
        }

        File ratings = this.files.newFile("ratings.csv");
        Files.writeString(ratings.toPath(), "userId,movieId,rating,timestamp\n" +
                                           "1,1,3.5,123\n", StandardCharsets.UTF_8);
        FileEdgeFetcher edgeFetcher = new FileEdgeFetcher(config);
        try {
            edgeFetcher.prepareLoadInputSplit(new FileInputSplit(
                    ElemType.EDGE, mapping.structs().get(2), ratings.getAbsolutePath()));
            Edge edge = edgeFetcher.next();
            Assert.assertEquals("rating", edge.label());
            Assert.assertEquals(3.5, edge.properties().get("rate"));
            Assert.assertNotNull(edge.id());
            Assert.assertFalse(edgeFetcher.hasNext());
        } finally {
            edgeFetcher.close();
        }
    }

    @Test
    public void testConstructor() {
        InputStruct inputStruct = Mockito.mock(InputStruct.class);
        FileInputSplit split = new FileInputSplit(ElemType.VERTEX, inputStruct,
                                                  "/tmp/test");
        Assert.assertEquals("/tmp/test", split.path());
        Assert.assertEquals(inputStruct, split.struct());
        Assert.assertSame(ElemType.VERTEX, split.type());
    }

    @Test
    public void testEquals() {
        InputStruct inputStruct = Mockito.mock(InputStruct.class);
        FileInputSplit split1 = new FileInputSplit(ElemType.VERTEX, inputStruct,
                                                   "/tmp/test");
        FileInputSplit split2 = new FileInputSplit(ElemType.VERTEX, inputStruct,
                                                   "/tmp/test");
        Assert.assertEquals(split1, split1);
        Assert.assertEquals(split1, split2);

        Assert.assertNotEquals(split1, null);
        Assert.assertNotEquals(split1, new Object());

        Assert.assertEquals(InputSplit.END_SPLIT, InputSplit.END_SPLIT);
        Assert.assertNotEquals(InputSplit.END_SPLIT, split1);
    }

    @Test
    public void testHashCode() {
        InputStruct inputStruct = Mockito.mock(InputStruct.class);
        FileInputSplit split1 = new FileInputSplit(ElemType.VERTEX, inputStruct,
                                                   "/tmp/test");
        FileInputSplit split2 = new FileInputSplit(ElemType.VERTEX, inputStruct,
                                                   "/tmp/test");
        Assert.assertEquals(split1.hashCode(), split2.hashCode());
    }
}
