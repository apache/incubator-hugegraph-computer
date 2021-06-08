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
import java.util.concurrent.atomic.AtomicReference;

import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.util.E;

/**
 * It's not a public class, need package access
 */
class SortedBufferQueue {

    private final BlockingQueue<QueuedMessage> queue;
    private final AtomicReference<Boolean> hungry;
    private final Runnable notEmptyNotifer;

    public SortedBufferQueue(AtomicReference<Boolean> hungry,
                             Runnable notEmptyNotifer) {
        E.checkArgumentNotNull(notEmptyNotifer,
                               "The callback to notify any queue not empty " +
                               "can't be null");
        // TODO: replace with conversant queue
        this.queue = new LinkedBlockingQueue<>(128);
        this.hungry = hungry;
        this.notEmptyNotifer = notEmptyNotifer;
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public void put(int partitionId, int workerId, MessageType type,
                    ByteBuffer buffer) throws InterruptedException {
        this.put(new QueuedMessage(partitionId, workerId, type, buffer));
    }

    public void put(QueuedMessage message) throws InterruptedException {
        this.queue.put(message);
        if (this.hungry.get()) {
            /*
             * Only invoke callback when send thread is hungry
             * to avoid frequently acquiring locks
             */
            this.notEmptyNotifer.run();
        }
    }

    public QueuedMessage peek() {
        return this.queue.peek();
    }

    public QueuedMessage take() throws InterruptedException {
        return this.queue.take();
    }
}
