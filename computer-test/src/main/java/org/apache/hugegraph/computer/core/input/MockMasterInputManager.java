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

public class MockMasterInputManager implements Manager {

    private InputSplitFetcher fetcher;
    private MasterInputHandler handler;

    public MockMasterInputManager() {
        this.fetcher = null;
        this.handler = null;
    }

    @Override
    public String name() {
        return "mock_master_input";
    }

    @Override
    public void init(Config config) {
        this.fetcher = InputSourceFactory.createInputSplitFetcher(config);
        this.handler = new MasterInputHandler(this.fetcher);
    }

    @Override
    public void close(Config config) {
        if (this.fetcher != null) {
            this.fetcher.close();
        }
    }

    public InputSplitFetcher fetcher() {
        return this.fetcher;
    }

    public MasterInputHandler handler() {
        return this.handler;
    }
}
