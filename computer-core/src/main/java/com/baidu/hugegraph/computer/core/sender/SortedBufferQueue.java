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

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import com.baidu.hugegraph.computer.core.network.message.MessageType;

public class SortedBufferQueue {

    private final Queue<SortedBufferMessage> queue;
    private final Runnable notifyNotEmpty;

    public SortedBufferQueue(Runnable notifyNotEmpty) {
        // TODO: replace with conversant queue
        this.queue = new LinkedBlockingQueue<>(128);
        this.notifyNotEmpty = notifyNotEmpty;
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public void offer(int partitionId, MessageType type, ByteBuffer buffer) {
        this.offer(new SortedBufferMessage(partitionId, type, buffer));
    }

    public void offer(SortedBufferMessage event) {
        this.queue.offer(event);
        this.notifyNotEmpty.run();
    }

    public SortedBufferMessage peek() {
        return this.queue.peek();
    }

    public SortedBufferMessage poll() {
        return this.queue.poll();
    }
}
