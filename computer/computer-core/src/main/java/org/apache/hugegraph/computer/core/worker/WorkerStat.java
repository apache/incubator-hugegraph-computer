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

package org.apache.hugegraph.computer.core.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.computer.core.graph.partition.PartitionStat;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.RandomAccessOutput;
import org.apache.hugegraph.computer.core.io.Writable;
import org.apache.hugegraph.computer.core.util.JsonUtil;
import org.apache.hugegraph.util.E;

public class WorkerStat implements Readable, Writable, Iterable<PartitionStat> {

    private int workerId;
    private List<PartitionStat> partitionStats;

    public WorkerStat() {
        this(0);
    }

    public WorkerStat(int workerId) {
        this.workerId = workerId;
        this.partitionStats = new ArrayList<>();
    }

    public int workerId() {
        return this.workerId;
    }

    public void add(PartitionStat stat) {
        E.checkArgumentNotNull(stat, "The stat can't be null");
        this.partitionStats.add(stat);
    }

    public PartitionStat get(int index) {
        return this.partitionStats.get(index);
    }

    public int size() {
        return this.partitionStats.size();
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        this.workerId = in.readInt();
        int size = in.readInt();
        this.partitionStats = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            PartitionStat stat = new PartitionStat();
            stat.read(in);
            this.partitionStats.add(stat);
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.workerId);
        out.writeInt(this.partitionStats.size());
        for (PartitionStat stat : this.partitionStats) {
            stat.write(out);
        }
    }

    @Override
    public Iterator<PartitionStat> iterator() {
        return this.partitionStats.iterator();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof WorkerStat)) {
            return false;
        }
        WorkerStat other = (WorkerStat) obj;
        return this.workerId == other.workerId &&
               this.partitionStats.equals(other.partitionStats);
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(this.workerId);
    }

    @Override
    public String toString() {
        return JsonUtil.toJsonWithClass(this);
    }
}
