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

package com.baidu.hugegraph.computer.core.receiver;

import java.util.ArrayList;
import java.util.List;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.google.common.collect.ImmutableList;

public class Buffers<T> {

    /*
     * The threshold is not hard limit. For performance, it checks
     * if the sumBytes >= threshold after {@link #addBuffer(T, long)}.
     */
    private long threshold;
    private long sumBytes;
    private List<T> list;
    private BarrierEvent event;
    private long sortTimeout;

    public Buffers(long threshold, long sortTimeout) {
        this.sumBytes = 0L;
        this.list = new ArrayList<>();
        this.event = new BarrierEvent();
        this.threshold = threshold;
        this.sortTimeout = sortTimeout;
    }

    public void addBuffer(T data, long size) {
        this.list.add(data);
        this.sumBytes += size;
    }

    public boolean isFull() {
        return this.sumBytes >= this.threshold;
    }

    public List<T> list() {
        return ImmutableList.copyOf(this.list);
    }

    /**
     * Wait the buffers to be sorted.
     */
    public void waitSorted() {
        if (this.list.size() == 0) {
            return;
        }
        try {
            boolean sorted = this.event.await(this.sortTimeout);
            if (!sorted) {
                throw new ComputerException("Not sorted in %s ms", this.sortTimeout);
            }
            this.event.reset();
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Interrupted while waiting buffers to be sorted", e);
        }
    }

    public void signalSorted() {
        this.list.clear();
        this.sumBytes = 0L;
        this.event.signalAll();
    }

    public long sumBytes() {
        return this.sumBytes;
    }
}
