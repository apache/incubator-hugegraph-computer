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

package org.apache.hugegraph.computer.core.output.hg.task;

import java.util.List;

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.output.hg.metrics.LoadSummary;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;

public class SingleInsertTask extends InsertTask {

    private static final Logger LOG = Log.logger(TaskManager.class);

    public SingleInsertTask(Config config, HugeClient client,
                            List<Vertex> batch, LoadSummary loadSummary) {
        super(config, client, batch, loadSummary);
    }

    @Override
    public void run() {
        for (Vertex vertex : this.batch) {
            try {
                this.insertSingle(vertex);
                this.increaseLoadSuccess();
            } catch (Exception e) {
                this.metrics().increaseInsertFailure();
                this.handleInsertFailure(e);
            }
        }
    }

    private void handleInsertFailure(Exception e) {
        LOG.error("Single insert error", e);
    }

    private void insertSingle(Vertex vertex) {
        this.insertBatch(ImmutableList.of(vertex));
    }
}
