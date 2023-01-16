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

package org.apache.hugegraph.computer.core.receiver;

import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.io.RandomAccessInput;
import org.apache.hugegraph.computer.core.io.UnsafeBytesInput;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.concurrent.BarrierEvent;

public class MessageRecvBuffers {

    /*
     * The bytesLimit is the limit of total bytes in buffers. The bytesLimit
     * is not hard limit. For performance, it checks whether totalBytes >=
     * bytesLimit after {@link #addBuffer(NetworkBuffer)}.
     * If totalBytes >= bytesLimit, the buffers will be sorted into a file.
     */
    private final long bytesLimit;
    private long totalBytes;

    private final List<byte[]> buffers;
    private final BarrierEvent sortFinished;
    private final long waitSortedTimeout;

    public MessageRecvBuffers(long bytesLimit, long waitSortedTimeout) {
        this.totalBytes = 0L;
        this.sortFinished = new BarrierEvent();
        this.bytesLimit = bytesLimit;
        this.waitSortedTimeout = waitSortedTimeout;
        this.buffers = new ArrayList<>();
    }

    public void addBuffer(NetworkBuffer data) {
        /*
         * TODO: don't not use copy, add a new class
         *       RandomAccessInput(NetworkBuffer)
         */
        byte[] bytes = data.copyToByteArray();
        this.buffers.add(bytes);
        this.totalBytes += bytes.length;
    }

    public boolean full() {
        return this.totalBytes >= this.bytesLimit;
    }

    /**
     * Get all the buffers.
     */
    public List<RandomAccessInput> buffers() {
        // Transfer byte[] list to BytesInput list
        List<RandomAccessInput> inputs = new ArrayList<>(this.buffers.size());
        for (byte[] buffer : this.buffers) {
            inputs.add(new UnsafeBytesInput(buffer));
        }
        return inputs;
    }

    /**
     * Prepare to sort the buffers, and reset event at the sort beginning
     */
    public void prepareSort() {
        // Reset buffers and totalBytes to prepare a new sorting
        this.buffers.clear();
        this.totalBytes = 0L;

        // Reset event to prepare a new sorting
        this.sortFinished.reset();
    }

    /**
     * Wait the buffers to be sorted.
     */
    public void waitSorted() {
        if (this.buffers.isEmpty()) {
            return;
        }
        try {
            boolean sorted = this.sortFinished.await(this.waitSortedTimeout);
            if (!sorted) {
                throw new ComputerException(
                          "Buffers have not been sorted in %s ms",
                          this.waitSortedTimeout);
            }
        } catch (InterruptedException e) {
            throw new ComputerException(
                      "Interrupted while waiting buffers to be sorted", e);
        }
    }

    /**
     * Set event and signal all waiting threads
     */
    public void signalSorted() {
        this.sortFinished.signalAll();
    }

    public long totalBytes() {
        return this.totalBytes;
    }
}
