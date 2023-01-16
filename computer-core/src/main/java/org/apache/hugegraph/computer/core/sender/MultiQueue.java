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

import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hugegraph.computer.core.common.Constants;

public class MultiQueue {

    private final Deque<QueuedMessage>[] deques;
    private int lastTakedQueueId;

    // TODO: use ReadWriteLock to optimize

    public MultiQueue(int size) {
        this(size, Constants.QUEUE_CAPACITY);
    }

    @SuppressWarnings("unchecked")
    public MultiQueue(int size, int capacityPerQueue) {
        this.deques = new ArrayDeque[size];
        for (int i = 0; i < size; i++) {
            this.deques[i] = new ArrayDeque<>(capacityPerQueue);
        }
        this.lastTakedQueueId = 0;
    }

    /**
     * Need ensure queueId start from 0
     */
    public synchronized void put(int queueId, QueuedMessage message)
                                 throws InterruptedException {
        Deque<QueuedMessage> deque = this.deques[queueId];
        deque.addLast(message);
        if (deque.size() == 1) {
            // Notify when the queue is empty before put
            this.notify();
        }
    }

    public synchronized void putAtFront(int queueId, QueuedMessage message) {
        Deque<QueuedMessage> deque = this.deques[queueId];
        deque.addFirst(message);
        if (deque.size() == 1) {
            // Notify when the queue is empty before put
            this.notify();
        }
    }

    public synchronized QueuedMessage take() throws InterruptedException {
        int traverse = 0;
        while (true) {
            Deque<QueuedMessage> deque = this.deques[this.lastTakedQueueId++];
            if (this.lastTakedQueueId == this.deques.length) {
                this.lastTakedQueueId = 0;
            }
            QueuedMessage message = deque.pollFirst();
            if (message != null) {
                return message;
            }
            if (++traverse >= this.deques.length) {
                // Block if all queue are empty
                this.wait();
                traverse = 0;
            }
        }
    }
}
