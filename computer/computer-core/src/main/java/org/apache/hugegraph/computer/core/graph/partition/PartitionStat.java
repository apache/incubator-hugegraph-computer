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

import java.io.IOException;

import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.io.Writable;
import org.apache.hugegraph.computer.core.receiver.MessageStat;
import org.apache.hugegraph.computer.core.util.JsonUtil;

public class PartitionStat implements Readable, Writable {

    private int partitionId;

    private long vertexCount;
    private long edgeCount;
    private long finishedVertexCount;

    private long messageSendCount;
    private long messageSendBytes;

    private long messageRecvCount;
    private long messageRecvBytes;

    public PartitionStat() {
        // For reflexion
        this(0, 0L, 0L, 0L);
    }

    public PartitionStat(int partitionId, long vertexCount, long edgeCount,
                         long finishedVertexCount) {
        this.partitionId = partitionId;
        this.vertexCount = vertexCount;
        this.edgeCount = edgeCount;
        this.finishedVertexCount = finishedVertexCount;
        this.messageSendCount = 0L;
        this.messageSendBytes = 0L;
        this.messageRecvCount = 0L;
        this.messageRecvBytes = 0L;
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

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.partitionId = in.readInt();
        this.vertexCount = in.readLong();
        this.edgeCount = in.readLong();
        this.finishedVertexCount = in.readLong();
        this.messageSendCount = in.readLong();
        this.messageSendBytes = in.readLong();
        this.messageRecvCount = in.readLong();
        this.messageRecvBytes = in.readLong();
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.partitionId);
        out.writeLong(this.vertexCount);
        out.writeLong(this.edgeCount);
        out.writeLong(this.finishedVertexCount);
        out.writeLong(this.messageSendCount);
        out.writeLong(this.messageSendBytes);
        out.writeLong(this.messageRecvCount);
        out.writeLong(this.messageRecvBytes);
    }

    public void mergeSendMessageStat(MessageStat messageStat) {
        this.messageSendCount += messageStat.messageCount();
        this.messageSendBytes += messageStat.messageBytes();
    }

    public void mergeRecvMessageStat(MessageStat messageStat) {
        this.messageRecvCount += messageStat.messageCount();
        this.messageRecvBytes += messageStat.messageBytes();
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
               this.messageSendCount == other.messageSendCount &&
               this.messageSendBytes == other.messageSendBytes &&
               this.messageRecvCount == other.messageRecvCount &&
               this.messageRecvBytes == other.messageRecvBytes;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(this.partitionId);
    }

    @Override
    public String toString() {
        return JsonUtil.toJsonWithClass(this);
    }
}
