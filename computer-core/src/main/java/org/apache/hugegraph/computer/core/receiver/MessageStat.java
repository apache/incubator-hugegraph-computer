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

package org.apache.hugegraph.computer.core.receiver;

/**
 * Send or Received message stat for a partition.
 */
public class MessageStat {

    private long messageCount;
    private long messageBytes;

    public MessageStat() {
        this(0L, 0L);
    }

    public MessageStat(long messageCount, long messageBytes) {
        this.messageCount = messageCount;
        this.messageBytes = messageBytes;
    }

    public long messageCount() {
        return this.messageCount;
    }

    public long messageBytes() {
        return this.messageBytes;
    }

    public void increase(MessageStat other) {
        this.messageCount += other.messageCount;
        this.messageBytes += other.messageBytes;
    }
}
