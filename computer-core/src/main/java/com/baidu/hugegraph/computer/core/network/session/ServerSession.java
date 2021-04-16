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
import com.baidu.hugegraph.computer.core.network.message.AbstractMessage;
import com.baidu.hugegraph.util.E;

public class ServerSession extends TransportSession {

    private final long minAckInterval;
    private volatile int maxHandledId;
    private volatile long lastAckTimestamp;

    public ServerSession(TransportConf conf) {
        super(conf);
        this.minAckInterval = this.conf().minAckInterval();
        this.maxHandledId = AbstractMessage.UNKNOWN_SEQ;
        this.lastAckTimestamp = 0;
    }

    @Override
    protected void ready() {
        this.maxHandledId = AbstractMessage.UNKNOWN_SEQ;
        this.lastAckTimestamp = 0;
        super.ready();
    }

    public void startRecv() {
        E.checkArgument(this.status == TransportStatus.READY,
                        "The status must be READY instead of %s " +
                        "on startRecv", this.status);
        this.maxRequestId = AbstractMessage.START_SEQ;
        this.status = TransportStatus.START_RECV;
    }

    public boolean finishRecv(int finishId) {
        E.checkArgument(this.status == TransportStatus.ESTABLISH,
                        "The status must be ESTABLISH instead of %s " +
                        "on finishRecv", this.status);
        E.checkArgument(finishId == this.maxRequestId + 1,
                        "The finishId must be auto-increment, finishId: %s, " +
                        "maxRequestId: %s", finishId, this.maxRequestId);

        this.finishId = finishId;
        this.status = TransportStatus.FINISH_RECV;
        return this.checkFinishReady();
    }

    @Override
    public void startComplete() {
        this.maxHandledId = AbstractMessage.START_SEQ;
        this.maxAckId = AbstractMessage.START_SEQ;
        this.establish();
    }

    @Override
    public void finishComplete() {
        this.ready();
    }

    public void dataRecv(int requestId) {
        E.checkArgument(this.status == TransportStatus.ESTABLISH,
                        "The status must be ESTABLISH instead of %s " +
                        "on dataRecv", this.status);
        E.checkArgument(requestId == this.maxRequestId + 1,
                        "The requestId must be auto-increment, requestId: %s," +
                        " maxRequestId: %s", requestId, this.maxRequestId);
        this.maxRequestId = requestId;
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

        return Pair.of(AckType.NONE, AbstractMessage.UNKNOWN_SEQ);
    }

    public void respondedAck(int ackId) {
        E.checkArgument(this.status == TransportStatus.ESTABLISH ||
                        this.status == TransportStatus.FINISH_RECV,
                        "The status must be ESTABLISH or FINISH_RECV instead " +
                        "of %s on respondedAck", this.status);
        E.checkArgument(ackId > this.maxAckId,
                        "The ackId must be increasing, ackId: %s, " +
                        "maxAckId: %s", ackId, this.maxAckId);
        this.maxAckId = ackId;
        this.lastAckTimestamp = System.currentTimeMillis();
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
        long interval = System.currentTimeMillis() - this.lastAckTimestamp;
        return interval >= this.minAckInterval &&
               this.maxHandledId > this.maxAckId;
    }
}
