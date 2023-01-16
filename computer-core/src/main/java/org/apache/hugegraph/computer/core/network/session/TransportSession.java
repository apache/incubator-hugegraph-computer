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

package org.apache.hugegraph.computer.core.network.session;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.TransportState;
import org.apache.hugegraph.computer.core.network.message.AbstractMessage;

public abstract class TransportSession {

    protected static final AtomicIntegerFieldUpdater<TransportSession>
              MAX_REQUEST_ID_UPDATER =
              AtomicIntegerFieldUpdater.newUpdater(TransportSession.class,
                                                   "maxRequestId");

    protected final TransportConf conf;
    protected volatile TransportState state;
    protected volatile int maxRequestId;
    protected volatile int maxAckId;
    protected volatile int finishId;

    protected TransportSession(TransportConf conf) {
        this.conf = conf;
        this.maxRequestId = AbstractMessage.UNKNOWN_SEQ;
        this.finishId = AbstractMessage.UNKNOWN_SEQ;
        this.maxAckId = AbstractMessage.UNKNOWN_SEQ;
        this.state = TransportState.READY;
    }

    protected void stateReady() {
        this.maxRequestId = AbstractMessage.UNKNOWN_SEQ;
        this.finishId = AbstractMessage.UNKNOWN_SEQ;
        this.maxAckId = AbstractMessage.UNKNOWN_SEQ;
        this.state = TransportState.READY;
    }

    protected void stateEstablished() {
        this.state = TransportState.ESTABLISHED;
    }

    public TransportState state() {
        return this.state;
    }

    public int nextRequestId() {
        return MAX_REQUEST_ID_UPDATER.incrementAndGet(this);
    }

    protected int genFinishId() {
        return this.maxRequestId + 1;
    }

    public int finishId() {
        return this.finishId;
    }

    public TransportConf conf() {
        return this.conf;
    }
}
