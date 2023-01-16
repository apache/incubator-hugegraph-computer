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

package org.apache.hugegraph.computer.core.output;

import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.vertex.Vertex;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class LimitedLogOutput extends AbstractComputerOutput {

    private static final Logger LOG = Log.logger(LimitedLogOutput.class);

    private static final String CONF_LIMIT_OUTPUT_PER_PARTITION_KEY =
                                "output.limit_logt_output";
    private static final int CONF_LIMIT_OUTPUT_PER_PARTITION_DEFAULT = 10;

    private int limit;
    private int logged;

    @Override
    public void init(Config config, int partition) {
        super.init(config, partition);
        this.limit = config.getInt(CONF_LIMIT_OUTPUT_PER_PARTITION_KEY,
                                   CONF_LIMIT_OUTPUT_PER_PARTITION_DEFAULT);
        this.logged = 0;
    }

    @Override
    public void write(Vertex vertex) {
        if (this.logged < this.limit) {
            LOG.info("'{}': '{}'", vertex.id(), vertex.value().string());
            this.logged++;
        }
    }

    @Override
    public void close() {
        LOG.info("End write back partition {}", this.partition());
    }
}
