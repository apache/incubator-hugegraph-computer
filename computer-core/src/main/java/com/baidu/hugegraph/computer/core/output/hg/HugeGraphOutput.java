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
import com.baidu.hugegraph.computer.core.output.AbstractComputerOutput;
import com.baidu.hugegraph.computer.core.output.hg.task.TaskManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.util.Log;

public abstract class HugeGraphOutput extends AbstractComputerOutput {

    private static final Logger LOG = Log.logger(HugeGraphOutput.class);

    private TaskManager taskManager;
    private List<com.baidu.hugegraph.structure.graph.Vertex> localVertices;
    private int batchSize;

    @Override
    public void init(Config config, int partition) {
        super.init(config, partition);

        this.taskManager = new TaskManager(config);
        this.batchSize = config.get(ComputerOptions.OUTPUT_BATCH_SIZE);
        this.localVertices = new ArrayList<>(this.batchSize);

        this.prepareSchema();
    }

    public HugeClient client() {
        return this.taskManager.client();
    }

    @Override
    public void write(Vertex vertex) {
        this.localVertices.add(this.constructHugeVertex(vertex));
        if (this.localVertices.size() >= this.batchSize) {
            this.commit();
        }
    }

    @Override
    public void close() {
        if (!this.localVertices.isEmpty()) {
            this.commit();
        }
        this.taskManager.waitFinished();
        this.taskManager.shutdown();
        LOG.info("End write back partition {}", this.partition());
    }

    protected void commit() {
        this.taskManager.submitBatch(this.localVertices);
        LOG.info("Write back {} vertices", this.localVertices.size());

        this.localVertices = new ArrayList<>(this.batchSize);
    }

    protected com.baidu.hugegraph.structure.graph.Vertex constructHugeVertex(
                                                         Vertex vertex) {
        com.baidu.hugegraph.structure.graph.Vertex hugeVertex =
                new com.baidu.hugegraph.structure.graph.Vertex(null);
        hugeVertex.id(vertex.id().asObject());
        hugeVertex.property(this.name(), this.value(vertex));
        return hugeVertex;
    }

    protected abstract void prepareSchema();

    protected abstract Object value(Vertex vertex);
}
