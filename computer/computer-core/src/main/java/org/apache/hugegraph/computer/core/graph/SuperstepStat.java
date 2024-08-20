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

package org.apache.hugegraph.computer.core.graph;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.hugegraph.computer.core.graph.partition.PartitionStat;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.io.Writable;
import org.apache.hugegraph.computer.core.util.JsonUtil;
import org.apache.hugegraph.computer.core.worker.WorkerStat;
import org.apache.hugegraph.util.E;

/**
 * It is sent from master to workers.
 */
public class SuperstepStat implements Readable, Writable {

    private long vertexCount;
    private long edgeCount;
    private long finishedVertexCount;

    private long messageSendCount;
    private long messageSendBytes;

    private long messageRecvCount;
    private long messageRecvBytes;

    private boolean active;

    public SuperstepStat() {
        this.active = true;
    }

    public void increase(PartitionStat partitionStat) {
        E.checkArgumentNotNull(partitionStat,
                               "The partitionStat can't be null");
        this.vertexCount += partitionStat.vertexCount();
        this.edgeCount += partitionStat.edgeCount();
        this.finishedVertexCount += partitionStat.finishedVertexCount();
        this.messageSendCount += partitionStat.messageSendCount();
        this.messageSendBytes += partitionStat.messageSendBytes();
        this.messageRecvCount += partitionStat.messageRecvCount();
        this.messageRecvBytes += partitionStat.messageRecvBytes();
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

    public long messageSendCount() {
        return this.messageSendCount;
    }

    public long messageSendBytes() {
        return this.messageSendBytes;
    }

    public long messageRecvCount() {
        return this.messageRecvCount;
    }

    public long messageRecvBytes() {
        return this.messageRecvBytes;
    }

    public void inactivate() {
        this.active = false;
    }

    public boolean active() {
        return this.active;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.vertexCount = in.readLong();
        this.edgeCount = in.readLong();
        this.finishedVertexCount = in.readLong();
        this.messageSendCount = in.readLong();
        this.messageSendBytes = in.readLong();
        this.messageRecvCount = in.readLong();
        this.messageRecvBytes = in.readLong();
        this.active = in.readBoolean();
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeLong(this.vertexCount);
        out.writeLong(this.edgeCount);
        out.writeLong(this.finishedVertexCount);
        out.writeLong(this.messageSendCount);
        out.writeLong(this.messageSendBytes);
        out.writeLong(this.messageRecvCount);
        out.writeLong(this.messageRecvBytes);
        out.writeBoolean(this.active);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SuperstepStat)) {
            return false;
        }
        SuperstepStat other = (SuperstepStat) obj;
        return this.vertexCount == other.vertexCount &&
               this.edgeCount == other.edgeCount &&
               this.finishedVertexCount == other.finishedVertexCount &&
               this.messageSendCount == other.messageSendCount &&
               this.messageSendBytes == other.messageSendBytes &&
               this.messageRecvCount == other.messageRecvCount &&
               this.messageRecvBytes == other.messageRecvBytes &&
               this.active == other.active;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.vertexCount, this.edgeCount,
                            this.finishedVertexCount,
                            this.messageSendCount, this.messageSendBytes,
                            this.messageRecvCount, this.messageRecvBytes,
                            this.active);
    }

    @Override
    public String toString() {
        return JsonUtil.toJsonWithClass(this);
    }

    public static SuperstepStat from(List<WorkerStat> workerStats) {
        SuperstepStat superstepStat = new SuperstepStat();
        for (WorkerStat workerStat : workerStats) {
            superstepStat.increase(workerStat);
        }
        return superstepStat;
    }
}
