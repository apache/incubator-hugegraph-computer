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

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.util.Log;

public class LimitLogOutput implements ComputerOutput {

    private static final Logger LOG = Log.logger(LimitLogOutput.class);
    private static final String CONF_LIMIT_OUTPUT_PER_PARTITION_KEY =
                                "output.limit_logt_output";
    private static final int CONF_LIMIT_OUTPUT_PER_PARTITION_DEFAULT = 10;

    private int partition;
    private int limit;
    private int index;

    @Override
    public void init(Config config, int partition) {
        this.partition = partition;
        this.limit = config.getInt(CONF_LIMIT_OUTPUT_PER_PARTITION_KEY,
                                   CONF_LIMIT_OUTPUT_PER_PARTITION_DEFAULT);
        this.index = 0;
        LOG.info("Start write back partition {}", this.partition);
    }

    @Override
    public void write(Vertex vertex) {
        if (this.index < this.limit) {
            LOG.info("id='{}', result='{}'", vertex.id(), vertex.value());
            this.index++;
        }
    }

    @Override
    public void close() {
        LOG.info("End write back partition {}", this.partition);
    }
}
