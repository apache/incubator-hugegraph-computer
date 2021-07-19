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
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.HugeClientBuilder;
import com.baidu.hugegraph.util.Log;

public abstract class HugeOutput implements ComputerOutput {

    private static final Logger LOG = Log.logger(PageRankOutput.class);

    private static final int BATCH_SIZE = 500;

    private int partition;
    private String url;
    private String graph;
    private HugeClient client;
    private List<com.baidu.hugegraph.structure.graph.Vertex> vertexBatch;

    @Override
    public void init(Config config, int partition) {
        LOG.info("Start write back partition {}", this.partition);

        this.partition = partition;
        this.url = config.get(ComputerOptions.HUGEGRAPH_URL);
        this.graph = config.get(ComputerOptions.HUGEGRAPH_GRAPH_NAME);
        this.client = new HugeClientBuilder(this.url, this.graph).build();
        this.vertexBatch = new ArrayList<>();

        // Prepare schema
        this.prepareSchema();
    }

    protected HugeClient client() {
        return this.client;
    }

    public abstract String name();

    public abstract void prepareSchema();

    @Override
    public void write(Vertex vertex) {
        this.vertexBatch.add(this.constructHugeVertex(vertex));
        if (this.vertexBatch.size() >= BATCH_SIZE) {
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
        this.client.close();
        LOG.info("End write back partition {}", this.partition);
    }

    private void commit() {
        this.client.graph().addVertices(this.vertexBatch);
        this.vertexBatch.clear();
        LOG.debug("Write back vertices {} of partition {} " +
                  "to graph: {} in url {} ",
                  this.vertexBatch, this.partition, this.url, this.graph);
    }
}
