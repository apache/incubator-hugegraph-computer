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

package com.baidu.hugegraph.computer.core.worker;

import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;

/**
 * This class used for test case. There are several managers mocked here.
 */
public class MockWorkerService extends WorkerService {

    public MockWorkerService() {
        super();
    }

    public void init(Config config) {
        super.init(config);
    }

    /**
     * @return WorkerStat mocked
     */
    @Override
    protected WorkerStat computePartitions() {
        PartitionStat stat1 = new PartitionStat(0, 100L, 200L,
                                                50L, 60L, 70L);
        WorkerStat workerStat = new WorkerStat();
        workerStat.add(stat1);
        return workerStat;
    }
}
