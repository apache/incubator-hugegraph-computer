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

import org.junit.Test;

import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;

public class SortedBufferQueueTest {

    @Test
    public void testIsEmpty() throws InterruptedException {
        SortedBufferQueue queue = new SortedBufferQueue();
        Assert.assertTrue(queue.isEmpty());

        queue.put(new QueuedMessage(1, 1, MessageType.START, null));
        Assert.assertFalse(queue.isEmpty());
    }

    @Test
    public void testPutAndTake() throws InterruptedException {
        SortedBufferQueue queue = new SortedBufferQueue();

        queue.put(1, 2, MessageType.VERTEX, ByteBuffer.allocate(4));
        queue.put(new QueuedMessage(2, 1, MessageType.EDGE,
                                    ByteBuffer.allocate(4)));

        BlockingQueue<?> blockQueue = Whitebox.getInternalState(queue, "queue");
        Assert.assertEquals(2, blockQueue.size());

        queue.take();
        Assert.assertEquals(1, blockQueue.size());

        queue.take();
        Assert.assertEquals(0, blockQueue.size());
    }
}
