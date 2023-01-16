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

package org.apache.hugegraph.computer.core.sort.sorting;

import org.apache.hugegraph.computer.core.common.ComputerContext;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;

public class RecvSortManager extends SortManager {

    private static final String NAME = "recv_sort";
    private static final String PREFIX = "recv-sort-executor-%s";

    public RecvSortManager(ComputerContext context) {
        super(context);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected String threadPrefix() {
        return PREFIX;
    }

    @Override
    protected Integer threadNum(Config config) {
        if (config.get(ComputerOptions.TRANSPORT_RECV_FILE_MODE)) {
            return 0;
        }
        return Math.min(super.threadNum(config),
                        this.maxSendSortThreads(config));
    }

    private int maxSendSortThreads(Config config) {
        Integer workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
        Integer partitions = config.get(ComputerOptions.JOB_PARTITIONS_COUNT);
        return partitions / workerCount;
    }
}
