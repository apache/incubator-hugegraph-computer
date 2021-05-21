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
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.UnsafeBytesInput;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.concurrent.BarrierEvent;

public class MessageRecvBuffers {

    /*
     * The threshold is not hard limit. For performance, it checks
     * if the sumBytes >= threshold after {@link #addBuffer(ManagedBuffer)}.
     */
    private long threshold;
    private long totalBytes;

    private List<byte[]> buffers;
    private BarrierEvent event;
    private long sortTimeout;

    public MessageRecvBuffers(long threshold, long sortTimeout) {
        this.totalBytes = 0L;
        this.event = new BarrierEvent();
        this.threshold = threshold;
        this.sortTimeout = sortTimeout;
        this.buffers = new ArrayList<>();
    }

    public void addBuffer(ManagedBuffer data) {
        /*
         * TODO: does not use copy. Develop new type of RandomAccessInput to
         *  direct read from ManagedBuffer.
         */
        byte[] bytes = data.copyToByteArray();
        this.buffers.add(bytes);
        this.totalBytes += bytes.length;
    }

    public boolean full() {
        return this.totalBytes >= this.threshold;
    }

    public List<RandomAccessInput> buffers() {
        List<RandomAccessInput> inputs = new ArrayList<>(this.buffers.size());
        for (byte[] buffer : this.buffers) {
            inputs.add(new UnsafeBytesInput(buffer));
        }
        return inputs;
    }

    /**
     * Wait the buffers to be sorted.
     */
    public void waitSorted() {
        if (this.buffers.size() == 0) {
            return;
        }
        try {
            boolean sorted = this.event.await(this.sortTimeout);
            if (!sorted) {
                throw new ComputerException("Buffers not sorted in %s ms",
                                            this.sortTimeout);
            }
            this.event.reset();
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Interrupted while waiting buffers to be sorted", e);
        }
    }

    public void signalSorted() {
        this.buffers.clear();
        this.totalBytes = 0L;
        this.event.signalAll();
    }

    public long totalBytes() {
        return this.totalBytes;
    }
}
