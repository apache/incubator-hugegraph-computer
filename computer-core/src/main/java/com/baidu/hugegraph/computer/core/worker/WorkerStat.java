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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.io.GraphInput;
import com.baidu.hugegraph.computer.core.io.GraphOutput;
import com.baidu.hugegraph.computer.core.io.Readable;
import com.baidu.hugegraph.computer.core.io.Writable;
import com.baidu.hugegraph.util.E;

public class WorkerStat implements Readable, Writable, Iterable<PartitionStat> {

    private int workerId;
    private List<PartitionStat> stats = new ArrayList<>();

    public WorkerStat() {
    }

    public WorkerStat(int workerId) {
        this.workerId = workerId;
    }

    public int workerId() {
        return this.workerId;
    }

    public void add(PartitionStat stat) {
        E.checkArgumentNotNull(stat, "The stat can't be null");
        this.stats.add(stat);
    }

    public PartitionStat get(int index) {
        return this.stats.get(index);
    }

    public int size() {
        return this.stats.size();
    }

    @Override
    public void read(GraphInput in) throws IOException {
        this.workerId = in.readInt();
        this.stats = new ArrayList<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            PartitionStat stat = new PartitionStat();
            stat.read(in);
            this.stats.add(stat);
        }
    }

    @Override
    public void write(GraphOutput out) throws IOException {
        out.writeInt(this.workerId);
        out.writeInt(this.stats.size());
        for (PartitionStat stat : this.stats) {
            stat.write(out);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof WorkerStat)) {
            return false;
        }
        WorkerStat other = (WorkerStat) obj;
        return this.workerId == other.workerId &&
               Arrays.equals(this.stats.toArray(), other.stats.toArray());
    }

    @Override
    public int hashCode() {
        int code = Integer.hashCode(this.workerId);
        code += Integer.hashCode(this.stats.size());
        for (PartitionStat stat : this.stats) {
            code += stat.hashCode();
        }
        return code;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("WorkerStat{").append("workerId=").append(this.workerId)
          .append(", partitionStats=[");
        for (int i = 0; i < this.stats.size(); i++) {
            sb.append(this.stats.get(i).toString());
            if (i != this.stats.size() - 1) {
                sb.append(",");
            }
        }
        sb.append("]}");
        return sb.toString();
    }

    @Override
    public Iterator<PartitionStat> iterator() {
        return this.stats.iterator();
    }
}
