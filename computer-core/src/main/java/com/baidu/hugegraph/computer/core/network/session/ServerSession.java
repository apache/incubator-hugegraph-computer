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

import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.TransportState;
import com.baidu.hugegraph.computer.core.network.message.AbstractMessage;
import com.baidu.hugegraph.util.E;

public class ServerSession extends TransportSession {

    private final long minAckInterval;
    private volatile int maxHandledId;

    public ServerSession(TransportConf conf) {
        super(conf);
        this.minAckInterval = this.conf().minAckInterval();
        this.maxHandledId = AbstractMessage.UNKNOWN_SEQ;
    }

    @Override
    protected void ready() {
        this.maxHandledId = AbstractMessage.UNKNOWN_SEQ;
        super.ready();
    }

    public void startRecv() {
        E.checkArgument(this.state == TransportState.READY,
                        "The state must be READY instead of %s " +
                        "at startRecv()", this.state);

        this.maxRequestId = AbstractMessage.START_SEQ;
        this.state = TransportState.START_RECV;
    }

    public boolean finishRecv(int finishId) {
        E.checkArgument(this.state == TransportState.ESTABLISH,
                        "The state must be ESTABLISH instead of %s " +
                        "at finishRecv()", this.state);
        E.checkArgument(finishId == this.maxRequestId + 1,
                        "The finishId must be maxRequestId + 1," +
                        " finishId: %s, maxRequestId: %s", finishId,
                        this.maxRequestId);

        this.finishId = finishId;
        this.state = TransportState.FINISH_RECV;
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
        E.checkArgument(this.state == TransportState.ESTABLISH,
                        "The state must be ESTABLISH instead of %s " +
                        "at dataRecv()", this.state);
        E.checkArgument(requestId == this.maxRequestId + 1,
                        "The requestId must be auto-increment, requestId: %s," +
                        " maxRequestId: %s", requestId, this.maxRequestId);

        this.maxRequestId = requestId;
    }

    public void handledData(int requestId) {
        E.checkArgument(this.state == TransportState.ESTABLISH ||
                        this.state == TransportState.FINISH_RECV,
                        "The state must be ESTABLISH or FINISH_RECV instead " +
                        "of %s on handledData", this.state);

        synchronized (this) {
            if (requestId > this.maxHandledId) {
                this.maxHandledId = requestId;
            }
        }
    }

    public void respondedDataAck(int ackId) {
        E.checkArgument(this.state == TransportState.ESTABLISH ||
                        this.state == TransportState.FINISH_RECV,
                        "The state must be ESTABLISH or FINISH_RECV instead " +
                        "of %s on respondedDataAck", this.state);
        E.checkArgument(ackId > this.maxAckId,
                        "The ackId must be increasing, ackId: %s, " +
                        "maxAckId: %s", ackId, this.maxAckId);

        this.maxAckId = ackId;
    }

    public boolean checkFinishReady() {
        if (this.state == TransportState.FINISH_RECV) {
            return this.maxHandledId == this.finishId - 1;
        }
        return false;
    }

    public boolean checkRespondDataAck() {
        if (this.state != TransportState.ESTABLISH) {
            return false;
        }
        return this.maxHandledId > this.maxAckId;
    }

    public int maxHandledId() {
        return this.maxHandledId;
    }

    public long minAckInterval() {
        return this.minAckInterval;
    }
}
