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

package com.baidu.hugegraph.computer.core.output.hg;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.ComputerOutput;
import com.baidu.hugegraph.computer.core.output.hg.task.TaskManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.util.Log;

public abstract class HugeOutput implements ComputerOutput {

    private static final Logger LOG = Log.logger(HugeOutput.class);

    private int partition;
    private TaskManager taskManager;
    private List<com.baidu.hugegraph.structure.graph.Vertex> vertexBatch;
    private int batchSize;

    @Override
    public void init(Config config, int partition) {
        LOG.info("Start write back partition {}", this.partition);

        this.partition = partition;

        this.taskManager = new TaskManager(config);
        this.vertexBatch = new ArrayList<>();
        this.batchSize = config.get(ComputerOptions.OUTPUT_BATCH_SIZE);

        this.prepareSchema();
    }

    public HugeClient client() {
        return this.taskManager.client();
    }

    public abstract String name();

    public abstract void prepareSchema();

    @Override
    public void write(Vertex vertex) {
        this.vertexBatch.add(this.constructHugeVertex(vertex));
        if (this.vertexBatch.size() >= this.batchSize) {
            this.commit();
        }
    }

    public abstract com.baidu.hugegraph.structure.graph.Vertex
                    constructHugeVertex(Vertex vertex);

    @Override
    public void close() {
        if (!this.vertexBatch.isEmpty()) {
            this.commit();
        }
        this.taskManager.waitFinished();
        this.taskManager.shutdown();
        LOG.info("End write back partition {}", this.partition);
    }

    private void commit() {
        this.taskManager.submitBatch(this.vertexBatch);
        LOG.info("Write back {} vertices", this.vertexBatch.size());

        this.vertexBatch = new ArrayList<>();
    }
}
