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

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * It's not a public class, need package access
 */
class MessageQueue {

    private final BlockingQueue<QueuedMessage> queue;
    private final Queue<QueuedMessage> pendings;

    public MessageQueue(int workerCount) {
        // TODO: replace with disruptor queue
        this.queue = new LinkedBlockingQueue<>(128);
        this.pendings = new ArrayDeque<>(workerCount);
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public void put(QueuedMessage message) throws InterruptedException {
        this.queue.put(message);
    }

    public void putBack(QueuedMessage message) {
        this.pendings.add(message);
    }

    public QueuedMessage take() throws InterruptedException {
        // Handle all pending messages first
        QueuedMessage pending = this.pendings.poll();
        if (pending != null) {
            return pending;
        }
        return this.queue.take();
    }
}
