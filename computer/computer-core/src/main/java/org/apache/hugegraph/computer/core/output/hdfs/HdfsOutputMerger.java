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

package org.apache.hugegraph.computer.core.output.hdfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;

public class HdfsOutputMerger {

    private FileSystem fs;
    private Path[] sourcePaths;
    private Path mergedPath;
    private static final String MERGED_FILE_NAME = "all.csv";

    protected HdfsOutputMerger() {
    }

    protected void init(Config config) {
        try {
            Configuration configuration = new Configuration();
            this.fs = HdfsOutput.openHDFS(config, configuration);

            String dir = config.get(ComputerOptions.OUTPUT_HDFS_DIR);
            String jobId = config.get(ComputerOptions.JOB_ID);
            int partitions = config.get(ComputerOptions.JOB_PARTITIONS_COUNT);
            this.sourcePaths = this.paths(dir, jobId, partitions);
            this.mergedPath = new Path(new Path(dir, jobId), MERGED_FILE_NAME);
        } catch (Exception e) {
            throw new ComputerException("Failed to init hdfs output merger", e);
        }
    }

    protected void merge() {
        try {
            this.fs.create(this.mergedPath, true).close();
            this.fs.concat(this.mergedPath, this.sourcePaths);
        } catch (IOException e) {
            throw new ComputerException("Failed to merge hdfs output files", e);
        }
    }

    private Path[] paths(String dir, String jobId, int partitions) throws
                                                                   IOException {
        List<Path> pathList = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            Path path = HdfsOutput.buildPath(dir, jobId, i);
            if (this.fs.exists(path) &&
                this.fs.getFileStatus(path).getLen() > 0) {
                pathList.add(path);
            }
        }
        return pathList.toArray(new Path[0]);
    }

    protected void close() {
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
