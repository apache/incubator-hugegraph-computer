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

package org.apache.hugegraph.computer.core.graph.partition;

import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.graph.id.Id;

public final class HashPartitioner implements Partitioner {

    private int partitionCount;
    private int workerCount;

    public HashPartitioner() {
        this.partitionCount = -1;
        this.workerCount = -1;
    }

    @Override
    public void init(Config config) {
        this.partitionCount = config.get(ComputerOptions.JOB_PARTITIONS_COUNT);
        this.workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
    }

    @Override
    public int partitionId(Id vertexId) {
        int hashCode = vertexId.hashCode() & Integer.MAX_VALUE;
        return hashCode % this.partitionCount;
    }

    @Override
    public int workerId(int partitionId) {
        // Note: workerId start from 1
        return (partitionId % this.workerCount) + 1;
    }
}
