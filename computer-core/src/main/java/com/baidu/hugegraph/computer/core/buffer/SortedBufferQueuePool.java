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

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.util.Log;

public class SortedBufferQueuePool {

    private static final Logger LOG = Log.logger(SortedBufferQueuePool.class);

    private final BarrierEvent notEmptyEvent;
    // workerId => queue
    private final SortedBufferQueue[] queues;

    public SortedBufferQueuePool(ComputerContext context) {
        Config config = context.config();
        int workerCount = config.get(ComputerOptions.JOB_WORKERS_COUNT);
        this.notEmptyEvent = new BarrierEvent();
        this.queues = new SortedBufferQueue[workerCount];
        for (int i = 0; i < workerCount; i++) {
            this.queues[i] = new SortedBufferQueue(this.notEmptyEvent);
        }
    }

    public SortedBufferQueue get(int workerId) {
        if (workerId < 0 || workerId >= this.queues.length)  {
            throw new ComputerException("Invalid workerId %s", workerId);

        }
        return this.queues[workerId];
    }

    public void waitUntilAnyQueueNotEmpty() {
        try {
            this.notEmptyEvent.await();
        } catch (InterruptedException e) {
            throw new ComputerException("Waiting any buffer queue not empty " +
                                        "was interrupted");
        }
    }
}
