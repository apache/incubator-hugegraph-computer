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

package com.baidu.hugegraph.computer.core.graph;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.util.JsonUtil;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.util.E;

/**
 * It is sent from master to workers.
 */
public class GraphStat implements Readable, Writable {

    private long vertexCount;
    private long edgeCount;
    private long finishedVertexCount;
    private long messageCount;
    private long messageBytes;
    private boolean halt;

    public GraphStat() {
    }

    public void increase(PartitionStat partitionStat) {
        E.checkArgumentNotNull(partitionStat,
                               "The partitionStat can't be null");
        this.vertexCount += partitionStat.vertexCount();
        this.edgeCount += partitionStat.edgeCount();
        this.finishedVertexCount += partitionStat.finishedVertexCount();
        this.messageCount += partitionStat.messageCount();
        this.messageBytes += partitionStat.messageBytes();
    }

    public void increase(WorkerStat workerStat) {
        E.checkArgumentNotNull(workerStat, "The workerStat can't be null");
        for (PartitionStat partitionStat : workerStat) {
            this.increase(partitionStat);
        }
    }

    public long vertexCount() {
        return this.vertexCount;
    }

    public long edgeCount() {
        return this.edgeCount;
    }

    public long finishedVertexCount() {
        return this.finishedVertexCount;
    }

    public long messageCount() {
        return this.messageCount;
    }

    public long messageBytes() {
        return this.messageBytes;
    }

    public boolean halt(boolean halt) {
        return this.halt = halt;
    }

    public boolean halt() {
        return this.halt;
    }

    @Override
    public void read(GraphInput in) throws IOException {
        this.vertexCount = in.readLong();
        this.edgeCount = in.readLong();
        this.finishedVertexCount = in.readLong();
        this.messageCount = in.readLong();
        this.messageBytes = in.readLong();
        this.halt = in.readBoolean();
    }

    @Override
    public void write(GraphOutput out) throws IOException {
        out.writeLong(this.vertexCount);
        out.writeLong(this.edgeCount);
        out.writeLong(this.finishedVertexCount);
        out.writeLong(this.messageCount);
        out.writeLong(this.messageBytes);
        out.writeBoolean(this.halt);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GraphStat)) {
            return false;
        }
        GraphStat other = (GraphStat) obj;
        return this.vertexCount == other.vertexCount &&
               this.edgeCount == other.edgeCount &&
               this.finishedVertexCount == other.finishedVertexCount &&
               this.messageCount == other.messageCount &&
               this.messageBytes == other.messageBytes &&
               this.halt == other.halt;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.vertexCount) +
               Long.hashCode(this.edgeCount) +
               Long.hashCode(this.messageCount) +
               Long.hashCode(this.edgeCount) +
               Long.hashCode(this.finishedVertexCount) +
               Long.hashCode(this.messageCount) +
               Long.hashCode(this.messageBytes) +
               Boolean.hashCode(this.halt);
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }
}
