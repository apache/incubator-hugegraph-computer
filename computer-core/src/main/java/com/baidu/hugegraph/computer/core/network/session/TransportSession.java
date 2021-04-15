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

package com.baidu.hugegraph.computer.core.network.session;

import java.util.concurrent.atomic.AtomicInteger;

import com.baidu.hugegraph.computer.core.network.TransportStatus;

public abstract class TransportSession {

    protected volatile TransportStatus status;
    protected final AtomicInteger maxRequestId;
    protected volatile int maxAckId;

    protected TransportSession() {
        this.status = TransportStatus.READY;
        this.maxRequestId = new AtomicInteger(-1);
        this.maxAckId = -1;
    }

    public TransportStatus status() {
        return this.status;
    }

    protected void ready() {
        this.maxRequestId.set(-1);
        this.status = TransportStatus.READY;
    }

    protected void establish() {
        this.status = TransportStatus.ESTABLISH;
    }

    public int maxRequestId() {
        return this.maxRequestId.get();
    }

    public int nextRequestId() {
        return this.maxRequestId.incrementAndGet();
    }

    public int incrementAckId() {
        return this.maxRequestId.incrementAndGet();
    }

    abstract void startComplete();

    abstract void finishComplete();
}
