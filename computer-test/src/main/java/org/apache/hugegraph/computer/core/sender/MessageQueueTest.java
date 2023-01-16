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

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Test;

public class MessageQueueTest {

    @Test
    public void testPutAndTake() throws InterruptedException {
        AtomicInteger notifyCounter = new AtomicInteger();
        MessageQueue queue = new MessageQueue(notifyCounter::incrementAndGet);

        QueuedMessage message1 = new QueuedMessage(1, MessageType.VERTEX,
                                                   ByteBuffer.allocate(4));
        // Trigger notifier called
        queue.put(message1);
        Assert.assertEquals(1, notifyCounter.get());

        QueuedMessage message2 = new QueuedMessage(2, MessageType.EDGE,
                                                   ByteBuffer.allocate(4));
        queue.put(message2);
        Assert.assertEquals(2, notifyCounter.get());

        BlockingQueue<?> blockQueue = Whitebox.getInternalState(queue, "queue");
        Assert.assertEquals(2, blockQueue.size());

        Assert.assertEquals(message1.partitionId(), queue.peek().partitionId());
        Assert.assertEquals(2, blockQueue.size());

        Assert.assertEquals(message1.partitionId(), queue.take().partitionId());
        Assert.assertEquals(1, blockQueue.size());

        Assert.assertEquals(message2.partitionId(), queue.take().partitionId());
        Assert.assertEquals(0, blockQueue.size());

        // Trigger notifier called
        queue.put(message1);
        Assert.assertEquals(3, notifyCounter.get());
    }
}
