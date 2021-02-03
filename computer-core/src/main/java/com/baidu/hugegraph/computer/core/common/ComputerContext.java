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

package com.baidu.hugegraph.computer.core.common;

import com.baidu.hugegraph.computer.core.allocator.Allocator;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.util.E;

public final class ComputerContext {

    private static volatile ComputerContext INSTANCE;

    private final Config config;
    private final GraphFactory factory;
    private final Allocator allocator;

    private ComputerContext(Config config) {
        this.config = config;
        this.factory = new GraphFactory();
        this.allocator = new Allocator(config);
    }

    public static synchronized void parseOptions(String... options) {
        Config config = new Config(options);
        INSTANCE = new ComputerContext(config);
    }

    public static ComputerContext instance() {
        E.checkNotNull(INSTANCE, "ComputerContext INSTANCE");
        return INSTANCE;
    }

    public Config config() {
        return this.config;
    }

    public GraphFactory graphFactory() {
        return this.factory;
    }

    public Allocator allocator() {
        return this.allocator;
    }
}
