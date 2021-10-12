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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;

public class HDFSOutputMerger {

    private FileSystem fs;
    private Path[] sourcePaths;
    private Path mergedPath;
    private static final String MERGED_FILE_NAME = "all.csv";

    public void init(Config config) {
        try {
            String dir = config.get(ComputerOptions.OUTPUT_HDFS_DIR);
            String jobId = config.get(ComputerOptions.JOB_ID);
            int partitions = config.get(ComputerOptions.JOB_PARTITIONS_COUNT);
            this.sourcePaths = this.paths(dir, jobId, partitions);
            this.mergedPath = new Path(new Path(dir, jobId), MERGED_FILE_NAME);

            Configuration configuration = new Configuration();
            String url = config.get(ComputerOptions.OUTPUT_HDFS_URL);
            String user = config.get(ComputerOptions.OUTPUT_HDFS_USER);
            this.fs = FileSystem.get(new URI(url), configuration, user);
        } catch (Exception e) {
            throw new ComputerException("Failed to init hdfs output merger", e);
        }
    }

    public void merge() {
        try {
            this.fs.create(this.mergedPath, true).close();
            this.fs.concat(this.mergedPath, this.sourcePaths);
        } catch (IOException e) {
            throw new ComputerException("Failed to merge hdfs output files", e);
        }
    }

    private Path[] paths(String dir, String jobId, int partitions) {
        Path[] paths = new Path[partitions];
        for (int i = 0; i < partitions; i++) {
            Path path = HDFSOutput.buildPath(dir, jobId, i);
            paths[i] = path;
        }
        return paths;
    }

    public void close() {
        try {
            if (this.fs != null) {
                this.fs.close();
            }
        } catch (IOException e) {
            throw new ComputerException("Failed to close hdfs output merger",
                                        e);
        }
    }
}
