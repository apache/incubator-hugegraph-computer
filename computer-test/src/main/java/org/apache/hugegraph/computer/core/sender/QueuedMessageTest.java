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

import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

public class QueuedMessageTest {

    @Test
    public void testGetter() {
        QueuedMessage message = new QueuedMessage(1, MessageType.VERTEX,
                                                  ByteBuffer.allocate(4));
        Assert.assertEquals(1, message.partitionId());
        Assert.assertEquals(MessageType.VERTEX, message.type());
        Assert.assertEquals(ByteBuffer.allocate(4), message.buffer());
    }
}
