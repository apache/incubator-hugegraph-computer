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

package com.baidu.hugegraph.computer.core.sender;

import java.io.IOException;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.io.OptimizedUnsafeBytesInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.network.message.MessageType;

public class WriteBuffers {

    // For writing
    private WriteBuffer writingBuffer;
    // For sorting
    private WriteBuffer sortingBuffer;

    public WriteBuffers(int size, int capacity) {
        this.writingBuffer = new WriteBuffer(size, capacity);
        this.sortingBuffer = new WriteBuffer(size, capacity);
    }

    public boolean reachThreshold() {
        return this.writingBuffer.reachThreshold();
    }

    public boolean isEmpty() {
        return this.writingBuffer.isEmpty();
    }

    public void writeVertex(MessageType type, Vertex vertex)
                            throws IOException {
        this.writingBuffer.writeVertex(type, vertex);
    }

    public synchronized void switchForSorting() {
        if (!this.reachThreshold()) {
            return;
        }
        // Ensure last sorting task finished
        while (!this.sortingBuffer.isEmpty()) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new ComputerException("Waiting sorting buffer empty " +
                                            "was interrupted");
            }
        }
        this.prepareSorting();
    }

    /**
     * Can remove synchronized if MessageSendManager.finish() only called by
     * single thread
     */
    public synchronized void prepareSorting() {
        // Swap the writing buffer and sorting buffer pointer
        WriteBuffer temp = this.writingBuffer;
        this.writingBuffer = this.sortingBuffer;
        this.sortingBuffer = temp;
    }

    public synchronized void finishSorting() {
        this.sortingBuffer.clear();
        this.notifyAll();
    }

    public RandomAccessInput wrapForRead() {
        return new OptimizedUnsafeBytesInput(this.sortingBuffer.output()
                                                               .buffer());
    }
}
