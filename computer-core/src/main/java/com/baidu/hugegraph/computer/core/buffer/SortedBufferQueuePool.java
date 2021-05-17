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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.util.Log;

public class SortedBufferQueuePool {

    private static final Logger LOG = Log.logger(SortedBufferQueuePool.class);

    // workerId => queue
    private final Map<Integer, SortedBufferQueue> queues;
    private final BarrierEvent notEmptyEvent;

    public SortedBufferQueuePool() {
        this.queues = new ConcurrentHashMap<>();
        this.notEmptyEvent = new BarrierEvent();
    }

    public SortedBufferQueue getOrCreateQueue(int workerId) {
        SortedBufferQueue queue = this.queues.get(workerId);
        if (queue == null) {
            SortedBufferQueue newQueue = new SortedBufferQueue(
                                         this.notEmptyEvent);
            queue = this.queues.putIfAbsent(workerId, newQueue);
            if (queue == null) {
                queue = newQueue;
                LOG.info("Create an SortedBufferQueue for worker {}", workerId);
            }
        }
        return queue;
    }

    public SortedBufferQueue get(int workerId) {
        return this.queues.get(workerId);
    }

    public void waitUntilAnyQueueNotEmpty() {
        try {
            this.notEmptyEvent.await();
        } catch (InterruptedException e) {
            throw new ComputerException("Waiting any buffer queue not empty" +
                                        "was interupted");
        }
    }
}
