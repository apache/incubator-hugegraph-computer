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

import org.apache.commons.lang3.tuple.Pair;

import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.TransportStatus;
import com.baidu.hugegraph.util.E;

public class ServerSession extends TransportSession {

    private volatile int maxHandledId;
    private volatile long lastAckTimestamp;

    public ServerSession(TransportConf conf) {
        super(conf);
        this.maxHandledId = SEQ_INIT_VALUE;
        this.lastAckTimestamp = 0;
    }

    private void startRecv() {
        this.maxRequestId = START_REQUEST_ID;
        this.status = TransportStatus.START_RECV;
    }

    private void finishRecv() {
        this.status = TransportStatus.FINISH_RECV;
    }

    @Override
    protected void ready() {
        this.maxHandledId = SEQ_INIT_VALUE;
        this.lastAckTimestamp = 0;
        super.ready();
    }

    @Override
    public void startComplete() {
        this.maxHandledId = START_REQUEST_ID;
        this.maxAckId = START_REQUEST_ID;
        this.establish();
    }

    @Override
    public void finishComplete() {
        this.ready();
    }

    public void receiveStart() {
        E.checkArgument(this.status == TransportStatus.READY,
                        "The status must be READY instead of %s " +
                        "on receiveStart", this.status);
        this.startRecv();
    }

    public boolean receiveFinish(int finishId) {
        E.checkArgument(this.status == TransportStatus.ESTABLISH,
                        "The status must be ESTABLISH instead of %s " +
                        "on receiveFinish", this.status);
        this.finishId = finishId;
        this.finishRecv();
        return this.checkFinishReady();
    }

    public void receivedData(int requestId) {
        E.checkArgument(this.status == TransportStatus.ESTABLISH,
                        "The status must be ESTABLISH instead of %s " +
                        "on receiveData", this.status);
        this.maxRequestId = requestId;
    }

    public void respondedAck(int ackId) {
        this.maxAckId = ackId;
        this.lastAckTimestamp = System.currentTimeMillis();
    }

    public synchronized Pair<AckType, Integer> handledData(int requestId) {
        E.checkArgument(this.status == TransportStatus.ESTABLISH ||
                        this.status == TransportStatus.FINISH_RECV,
                        "The status must be ESTABLISH or FINISH_RECV instead " +
                        "of %s on handledData", this.status);
        if (requestId > this.maxHandledId) {
            this.maxHandledId = requestId;
        }

        if (this.status == TransportStatus.FINISH_RECV &&
            this.checkFinishReady()) {
            return Pair.of(AckType.FINISH, this.finishId);
        }

        if (this.status == TransportStatus.ESTABLISH &&
            this.checkRespondAck()) {
            return Pair.of(AckType.DATA, this.maxHandledId);
        }

        return Pair.of(AckType.NONE, SEQ_INIT_VALUE);
    }

    private boolean checkFinishReady() {
        if (this.status == TransportStatus.READY) {
            return true;
        }
        if (this.status == TransportStatus.FINISH_RECV) {
            return this.maxHandledId == this.finishId - 1;
        }
        return false;
    }

    private boolean checkRespondAck() {
        long minAckInterval = this.conf.minAckInterval();
        long interval = System.currentTimeMillis() - this.lastAckTimestamp;
        return interval >= minAckInterval;
    }
}
