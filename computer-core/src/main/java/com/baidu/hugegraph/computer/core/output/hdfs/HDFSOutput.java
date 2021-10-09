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

package com.baidu.hugegraph.computer.core.output.hdfs;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.ComputerOutput;
import com.baidu.hugegraph.computer.core.util.StringEncoding;

public class HDFSOutput implements ComputerOutput {

    private FileSystem fs;
    private FSDataOutputStream fileOutputStream;
    private String delimiter;
    private static final String REPLICATION_KEY = "dfs.replication";
    private static final String FILE_SUFFIX = ".csv";
    private static final String VERTEX_ID_COLUMN = "vertexId";
    private static final String RESULT_COLUMN = "result";

    @Override
    public void init(Config config, int partition) {
        try {
            this.delimiter = config.get(ComputerOptions.OUTPUT_HDFS_DELIMITER);
            this.openHDFS(config, partition);
            this.writeHeaders();
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new ComputerException("Failed to init hdfs output on " +
                                        "partition [%s]", e, partition);
        }
    }

    private void openHDFS(Config config, int partition) throws
                                                        IOException,
                                                        URISyntaxException,
                                                        InterruptedException {
        Configuration configuration = new Configuration();
        short replication = config.get(ComputerOptions.OUTPUT_HDFS_REPLICATION);
        configuration.set(REPLICATION_KEY, String.valueOf(replication));
        String url = config.get(ComputerOptions.OUTPUT_HDFS_URL);
        String user = config.get(ComputerOptions.OUTPUT_HDFS_USER);
        this.fs = FileSystem.get(new URI(url), configuration, user);

        String dir = config.get(ComputerOptions.OUTPUT_HDFS_DIR);
        String jobId = config.get(ComputerOptions.JOB_ID);
        String filePath = this.path(dir, jobId, partition);
        Path hdfsPath = new Path(filePath);
        this.fileOutputStream = this.fs.create(hdfsPath, true);
    }

    protected void writeHeaders() throws IOException {
        this.writeString(VERTEX_ID_COLUMN);
        this.writeString(this.delimiter);
        this.writeString(RESULT_COLUMN);
        this.writeString(System.lineSeparator());
    }

    @Override
    public void write(Vertex vertex) {
        try {
            this.writeString(vertex.id().toString());
            this.writeString(this.delimiter);
            this.writeString(this.constructValueString(vertex));
            this.writeString(System.lineSeparator());
        } catch (IOException e) {
            throw new ComputerException("Failed to write vertex: {}",
                                        vertex.toString(), e);
        }
    }

    protected void writeBytes(byte[] bytes) throws IOException {
        this.fileOutputStream.write(bytes);
    }

    protected void writeString(String string) throws IOException {
        this.writeBytes(StringEncoding.encode(string));
    }

    protected String constructValueString(Vertex vertex) {
        return vertex.value().toString();
    }

    private String path(String dir, String jobId, int partition) {
        if (dir.endsWith(Path.SEPARATOR)) {
            return dir + jobId + Path.SEPARATOR + partition + FILE_SUFFIX;
        } else {
            return dir + Path.SEPARATOR + jobId + Path.SEPARATOR
                       + partition + FILE_SUFFIX;
        }
    }

    @Override
    public void close() {
        try {
            if (this.fileOutputStream != null) {
                this.fileOutputStream.close();
            }
            if (this.fs != null) {
                this.fs.close();
            }
        } catch (IOException e) {
            throw new ComputerException("Failed to close hdfs", e);
        }
    }
}
