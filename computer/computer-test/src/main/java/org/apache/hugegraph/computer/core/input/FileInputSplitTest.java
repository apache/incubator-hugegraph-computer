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
import java.util.Set;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.input.loader.FileEdgeFetcher;
import org.apache.hugegraph.computer.core.input.loader.FileInputSplit;
import org.apache.hugegraph.computer.core.input.loader.FileVertxFetcher;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.loader.constant.ElemType;
import org.apache.hugegraph.loader.mapping.InputStruct;
import org.apache.hugegraph.loader.mapping.LoadMapping;
import org.apache.hugegraph.loader.util.JsonUtil;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class FileInputSplitTest extends UnitTestBase {

    @Rule
    public TemporaryFolder files = new TemporaryFolder();

    @Test
    public void testOfflineVertexAndEdgeInput() throws Exception {
        LoadMapping mapping = this.offlineMapping(false);
        Config config = this.offlineConfig();

        File users = this.files.newFile("users.csv");
        Files.writeString(users.toPath(), "userId\n1\n2\n", StandardCharsets.UTF_8);
        FileVertxFetcher vertexFetcher = new FileVertxFetcher(config);
        Object userId;
        try {
            vertexFetcher.prepareLoadInputSplit(new FileInputSplit(
                    ElemType.VERTEX, mapping.structs().get(0), users.getAbsolutePath()));
            Vertex first = vertexFetcher.next();
            Vertex second = vertexFetcher.next();
            Assert.assertEquals("user", first.label());
            Assert.assertEquals(1, first.properties().get("id"));
            Assert.assertEquals(2, second.properties().get("id"));
            userId = first.id();
            Assert.assertNotNull(userId);
            Assert.assertFalse(vertexFetcher.hasNext());
        } finally {
            vertexFetcher.close();
        }

        File movies = this.files.newFile("movies.csv");
        Files.writeString(movies.toPath(), "movieId,title,genres\n1,Toy Story,Animation|Comedy\n",
                          StandardCharsets.UTF_8);
        FileVertxFetcher movieFetcher = new FileVertxFetcher(config);
        Object movieId;
        try {
            movieFetcher.prepareLoadInputSplit(new FileInputSplit(
                    ElemType.VERTEX, mapping.structs().get(1), movies.getAbsolutePath()));
            Vertex movie = movieFetcher.next();
            movieId = movie.id();
            Assert.assertEquals("2:11", movieId);
            Assert.assertEquals(1, movie.properties().get("id"));
            Assert.assertEquals("Toy Story", movie.properties().get("title"));
            Assert.assertEquals(Set.of("Animation", "Comedy"), movie.properties().get("genres"));
            Assert.assertFalse(movieFetcher.hasNext());
        } finally {
            movieFetcher.close();
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
            Assert.assertEquals(userId, edge.sourceId());
            Assert.assertEquals(movieId, edge.targetId());
            Assert.assertFalse(edgeFetcher.hasNext());
        } finally {
            edgeFetcher.close();
        }
    }

    @Test
    public void testOfflineUnfoldedPrimaryKeyVertices() throws Exception {
        LoadMapping mapping = this.offlineMapping(true);
        File movies = this.files.newFile("movies.csv");
        Files.writeString(movies.toPath(), "movieId,title,genres\n1||2,Toy Story,Animation|Comedy\n",
                          StandardCharsets.UTF_8);
        FileVertxFetcher fetcher = new FileVertxFetcher(this.offlineConfig());
        try {
            fetcher.prepareLoadInputSplit(new FileInputSplit(
                    ElemType.VERTEX, mapping.structs().get(1), movies.getAbsolutePath()));
            for (int id = 1; id <= 2; id++) {
                Vertex movie = fetcher.next();
                Assert.assertEquals("2:1" + id, movie.id());
                Assert.assertEquals(id, movie.properties().get("id"));
                Assert.assertEquals("Toy Story", movie.properties().get("title"));
                Assert.assertEquals(Set.of("Animation", "Comedy"), movie.properties().get("genres"));
            }
            Assert.assertFalse(fetcher.hasNext());
        } finally {
            fetcher.close();
        }
    }

    private LoadMapping offlineMapping(boolean unfold) throws Exception {
        String json = Files.readString(Paths.get("src/main/resources/hdfs_input_test/struct.json"))
                           .replace("\"HDFS\"", "\"FILE\"")
                           .replace("\"core_site_path\": \"src/main/resources/hdfs_input_test/core-site.xml\",", "");
        JsonNode mapping = JsonUtil.fromJson(json, JsonNode.class);
        ((ObjectNode) mapping.path("structs").get(1).path("vertices").get(0)).put("unfold", unfold);
        File file = this.files.newFile("struct.json");
        Files.writeString(file.toPath(), mapping.toString(), StandardCharsets.UTF_8);
        return LoadMapping.of(file.getAbsolutePath());
    }

    private Config offlineConfig() {
        Config config = Mockito.mock(Config.class);
        Mockito.when(config.get(ComputerOptions.INPUT_LOADER_SCHEMA_PATH))
               .thenReturn("src/main/resources/hdfs_input_test/schema.json");
        return config;
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
