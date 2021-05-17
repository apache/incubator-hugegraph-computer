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

package com.baidu.hugegraph.computer.core.buffer;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.worker.SortedBufferEvent;
import com.baidu.hugegraph.concurrent.BarrierEvent;

public class SortedBufferQueue {

    private final Queue<SortedBufferEvent> queue;
    private final BarrierEvent notEmptyEvent;

    public SortedBufferQueue(BarrierEvent barrierEvent) {
        // TODO: replace with conversant queue
        this.queue = new LinkedBlockingQueue<>(128);
        this.notEmptyEvent = barrierEvent;
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public void offer(int partitionId, MessageType type, ByteBuffer buffer) {
        this.queue.offer(new SortedBufferEvent(partitionId, type, buffer));
        this.notEmptyEvent.signalAll();
    }

    public SortedBufferEvent peek() {
        return this.queue.peek();
    }

    public SortedBufferEvent poll() {
        return this.queue.poll();
    }
}
