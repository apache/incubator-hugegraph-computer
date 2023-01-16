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
import java.util.Set;

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.output.hg.metrics.LoadMetrics;
import org.apache.hugegraph.computer.core.output.hg.metrics.LoadSummary;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.graph.Vertex;

import com.google.common.collect.ImmutableSet;

public abstract class InsertTask implements Runnable {

    public static final Set<String> UNACCEPTABLE_EXCEPTIONS = ImmutableSet.of(
            "class java.lang.IllegalArgumentException"
    );

    public static final String[] UNACCEPTABLE_MESSAGES = {
            // org.apache.http.conn.HttpHostConnectException
            "Connection refused",
            "The server is being shutting down",
            "not allowed to insert, because already exist a vertex " +
            "with same id and different label"
    };

    protected Config config;
    private HugeClient client;
    protected final List<Vertex> batch;
    private LoadSummary summary;

    public InsertTask(Config config, HugeClient client,
                      List<Vertex> batch, LoadSummary loadSummary) {
        this.config = config;
        this.client = client;
        this.batch = batch;
        this.summary = loadSummary;
    }

    public LoadSummary summary() {
        return this.summary;
    }

    public LoadMetrics metrics() {
        return this.summary().metrics();
    }

    protected void plusLoadSuccess(int count) {
        LoadMetrics metrics = this.summary().metrics();
        metrics.plusInsertSuccess(count);
        this.summary().plusLoaded(count);
    }

    protected void increaseLoadSuccess() {
        this.plusLoadSuccess(1);
    }

    protected void insertBatch(List<Vertex> vertices) {
        this.client.graph().addVertices(vertices);
    }
}
