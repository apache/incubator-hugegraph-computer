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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.util.E;

/**
 * It's not a public class, need package access
 */
class SortedBufferQueue {

    private final BlockingQueue<SortedBufferMessage> queue;
    private final Runnable notEmptyNotifer;
    private final AtomicBoolean empty;

    public SortedBufferQueue(Runnable notEmptyNotifer) {
        E.checkArgumentNotNull(notEmptyNotifer,
                               "The callback to notify any queue not empty " +
                               "can't be null");
        // TODO: replace with conversant queue
        this.queue = new LinkedBlockingQueue<>(128);
        this.notEmptyNotifer = notEmptyNotifer;
        this.empty = new AtomicBoolean(true);
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public void put(int partitionId, MessageType type, ByteBuffer buffer)
                    throws InterruptedException {
        this.put(new SortedBufferMessage(partitionId, type, buffer));
    }

    public void put(SortedBufferMessage message) throws InterruptedException {
        this.queue.put(message);
        if (this.empty.compareAndSet(true, false)) {
            /*
             * Only invoke callback when queue from empty to not empty
             * to avoid frequently acquiring locks
             */
            this.notEmptyNotifer.run();
        }
    }

    public SortedBufferMessage peek() {
        SortedBufferMessage message = this.queue.peek();
        if (message == null) {
            this.empty.compareAndSet(false, true);
        }
        return message;
    }

    public SortedBufferMessage take() throws InterruptedException {
        return this.queue.take();
    }
}
