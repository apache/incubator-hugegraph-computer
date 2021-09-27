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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.ComputerOutput;

public class HDFSOutput implements ComputerOutput {

    private FileSystem fs;
    private FSDataInputStream inputStream;

    @Override
    public void init(Config config, int partition) {
        Configuration configuration= new Configuration();
        try {
            this.fs = FileSystem.get(configuration);
            this.inputStream = this.fs.open(new Path(""));
        } catch (IOException e) {
            throw new ComputerException("Failed to open hdfs", e);
        }
    }

    @Override
    public void write(Vertex vertex) {
        //FSDataOutputStream append = this.fs.append();
        //append.write();
    }

    @Override
    public void close() {
        try {
            this.inputStream.close();
            this.fs.close();
        } catch (IOException e) {
            throw new ComputerException("Failed to close hdfs", e);
        }
    }
}
