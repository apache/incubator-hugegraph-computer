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

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class MasterInputManager implements Manager {

    private static final Logger LOG = Log.logger(MasterInputManager.class);

    public static final String NAME = "master_input";

    private InputSplitFetcher fetcher;
    private MasterInputHandler handler;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void init(Config config) {
        this.fetcher = InputSourceFactory.createInputSplitFetcher(config);
        this.handler = new MasterInputHandler(this.fetcher);
        int vertexSplitSize = this.handler.createVertexInputSplits();
        int edgeSplitSize = this.handler.createEdgeInputSplits();
        LOG.info("Master create {} vertex splits, {} edge splits",
                 vertexSplitSize, edgeSplitSize);
    }

    @Override
    public void close(Config config) {
        // Maybe fetcher=null if not successfully initialized
        if (this.fetcher != null) {
            this.fetcher.close();
        }
    }

    public MasterInputHandler handler() {
        E.checkNotNull(this.handler, "handler");
        return this.handler;
    }
}
