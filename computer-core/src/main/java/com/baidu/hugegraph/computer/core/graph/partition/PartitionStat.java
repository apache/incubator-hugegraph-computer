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

package com.baidu.hugegraph.computer.core.graph.partition;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.Writable;

public class PartitionStat implements Readable, Writable {

    private int partitionId;
    private long vertexCount;
    private long edgeCount;
    private long finishedVertexCount;
    private long messageCount;
    private long messageBytes;

    // For reflexion
    public PartitionStat() {
    }

    // For data load phase
    public PartitionStat(int partitionId, long vertexCount, long edgeCount) {
        this.partitionId = partitionId;
        this.vertexCount = vertexCount;
        this.edgeCount = edgeCount;
    }

    public PartitionStat(int partitionId, long vertexCount, long edgeCount,
                         long finishedVertexCount, long messageCount,
                         long messageBytes) {
        this.partitionId = partitionId;
        this.vertexCount = vertexCount;
        this.edgeCount = edgeCount;
        this.finishedVertexCount = finishedVertexCount;
        this.messageCount = messageCount;
        this.messageBytes = messageBytes;
    }

    public int partitionId() {
        return this.partitionId;
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

    @Override
    public void read(GraphInput in) throws IOException {
        this.partitionId = in.readInt();
        this.vertexCount = in.readLong();
        this.edgeCount = in.readLong();
        this.finishedVertexCount = in.readLong();
        this.messageCount = in.readLong();
        this.messageBytes = in.readLong();
    }

    @Override
    public void write(GraphOutput out) throws IOException {
        out.writeInt(this.partitionId);
        out.writeLong(this.vertexCount);
        out.writeLong(this.edgeCount);
        out.writeLong(this.finishedVertexCount);
        out.writeLong(this.messageCount);
        out.writeLong(this.messageBytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PartitionStat)) {
            return false;
        }
        PartitionStat other = (PartitionStat) obj;
        return this.partitionId == other.partitionId &&
               this.vertexCount == other.vertexCount &&
               this.finishedVertexCount == other.finishedVertexCount &&
               this.edgeCount == other.edgeCount &&
               this.messageCount == other.messageCount &&
               this.messageBytes == other.messageBytes;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(this.partitionId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PartitionStat{partitionId=").append(this.partitionId)
          .append(", vertexCount=").append(this.vertexCount)
          .append(", edgeCount=").append(this.edgeCount)
          .append(", finishedVertexCount=").append(this.finishedVertexCount)
          .append(", messageCount=").append(this.messageCount)
          .append(", messageBytes=").append(this.messageBytes)
          .append("}");
        return sb.toString();
    }
}
