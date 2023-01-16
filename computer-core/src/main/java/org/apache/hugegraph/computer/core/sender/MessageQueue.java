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

package org.apache.hugegraph.computer.core.sender;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hugegraph.util.E;

/**
 * It's not a public class, need package access
 */
class MessageQueue {

    private final BlockingQueue<QueuedMessage> queue;
    private final Runnable notEmptyNotifier;

    public MessageQueue(Runnable notEmptyNotifier) {
        E.checkArgumentNotNull(notEmptyNotifier,
                               "The callback to notify that a queue is " +
                               "not empty can't be null");
        // TODO: replace with disruptor queue
        this.queue = new LinkedBlockingQueue<>(128);
        this.notEmptyNotifier = notEmptyNotifier;
    }

    public void put(QueuedMessage message) throws InterruptedException {
        this.queue.put(message);
        /*
         * TODO: Try to optimize, don't signal every time when the queue
         * is not empty
         */
        this.notEmptyNotifier.run();
    }

    public QueuedMessage peek() {
        return this.queue.peek();
    }

    public QueuedMessage take() throws InterruptedException {
        return this.queue.take();
    }
}
