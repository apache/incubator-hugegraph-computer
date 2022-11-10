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

package com.baidu.hugegraph.computer.core.output;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.worker.Computation;
import org.apache.hugegraph.util.Log;

public abstract class AbstractComputerOutput implements ComputerOutput {

    private static final Logger LOG = Log.logger(ComputerOutput.class);

    private String name;
    private int partition;

    @Override
    public void init(Config config, int partition) {
        Computation<?> computation = config.createObject(
                                     ComputerOptions.WORKER_COMPUTATION_CLASS);
        this.name = computation.name();
        this.partition = partition;

        LOG.info("Start write back partition {} for {}",
                 this.partition(), this.name());
    }

    @Override
    public String name() {
        return this.name;
    }

    public int partition() {
        return this.partition;
    }
}
