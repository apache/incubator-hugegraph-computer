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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;

public class SortedBufferQueueTest {

    @Test
    public void testConstructor() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new SortedBufferQueue(null);
        });

        BarrierEvent event = new BarrierEvent();
        @SuppressWarnings("unused")
        SortedBufferQueue queue = new SortedBufferQueue(event::signal);
    }

    @Test
    public void testIsEmpty() throws InterruptedException {
        BarrierEvent event = new BarrierEvent();
        SortedBufferQueue queue = new SortedBufferQueue(event::signal);
        Assert.assertTrue(queue.isEmpty());

        queue.put(SortedBufferMessage.START);
        Assert.assertFalse(queue.isEmpty());
    }

    @Test
    public void testPutAndTake() throws InterruptedException {
        BarrierEvent event = new BarrierEvent();
        AtomicInteger counter = new AtomicInteger();
        SortedBufferQueue queue = new SortedBufferQueue(() -> {
            event.signal();
            counter.incrementAndGet();
        });

        AtomicBoolean empty = Whitebox.getInternalState(queue, "empty");
        Assert.assertTrue(empty.get());
        queue.put(1, MessageType.VERTEX, ByteBuffer.allocate(4));
        Assert.assertFalse(empty.get());
        // This time will not trigger callback
        queue.put(new SortedBufferMessage(2, MessageType.EDGE,
                                          ByteBuffer.allocate(4)));
        Assert.assertEquals(1, counter.get());

        SortedBufferMessage message = queue.take();
        Assert.assertEquals(1, message.partitionId());
        message = queue.take();
        Assert.assertEquals(2, message.partitionId());

        Assert.assertNull(queue.peek());
        Assert.assertTrue(empty.get());

        queue.put(3, MessageType.VERTEX, ByteBuffer.allocate(4));
        Assert.assertEquals(2, counter.get());
    }
}
