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

import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.TransportState;
import org.apache.hugegraph.computer.core.network.message.AbstractMessage;
import org.apache.hugegraph.util.E;

public class ServerSession extends TransportSession {

    private final long minAckInterval;
    private volatile int maxHandledId;

    public ServerSession(TransportConf conf) {
        super(conf);
        this.minAckInterval = this.conf().minAckInterval();
        this.maxHandledId = AbstractMessage.UNKNOWN_SEQ;
    }

    @Override
    protected void stateReady() {
        this.maxHandledId = AbstractMessage.UNKNOWN_SEQ;
        super.stateReady();
    }

    public void completeStateStart() {
        E.checkArgument(this.state == TransportState.START_RECV,
                        "The state must be START_RECV instead of %s " +
                        "at completeStateStart()", this.state);

        this.maxHandledId = AbstractMessage.START_SEQ;
        this.maxAckId = AbstractMessage.START_SEQ;
        this.stateEstablished();
    }

    public void completeStateFinish() {
        E.checkArgument(this.state == TransportState.FINISH_RECV,
                        "The state must be FINISH_RECV instead of %s " +
                        "at completeStateFinish()", this.state);

        this.stateReady();
    }

    public void onRecvStateStart() {
        E.checkArgument(this.state == TransportState.READY,
                        "The state must be READY instead of %s " +
                        "at onRecvStateStart()", this.state);

        this.maxRequestId = AbstractMessage.START_SEQ;
        this.state = TransportState.START_RECV;
    }

    public boolean onRecvStateFinish(int finishId) {
        E.checkArgument(this.state == TransportState.ESTABLISHED,
                        "The state must be ESTABLISHED instead of %s " +
                        "at onRecvStateFinish()", this.state);
        E.checkArgument(finishId == this.maxRequestId + 1,
                        "The finishId must be maxRequestId + 1 at " +
                        "onRecvStateFinish(), finishId: %s, maxRequestId: %s",
                        finishId, this.maxRequestId);

        this.finishId = finishId;
        this.state = TransportState.FINISH_RECV;
        return this.needAckFinish();
    }

    public void onRecvData(int requestId) {
        E.checkArgument(this.state == TransportState.ESTABLISHED,
                        "The state must be ESTABLISHED instead of %s " +
                        "at onRecvData()", this.state);
        E.checkArgument(requestId == this.maxRequestId + 1,
                        "The requestId must be increasing at onRecvData(), " +
                        "requestId: %s, maxRequestId: %s", requestId,
                        this.maxRequestId);

        this.maxRequestId = requestId;
    }

    public void onHandledData(int requestId) {
        E.checkArgument(this.state == TransportState.ESTABLISHED ||
                        this.state == TransportState.FINISH_RECV,
                        "The state must be ESTABLISHED or FINISH_RECV " +
                        "instead of %s at onHandledData()", this.state);

        synchronized (this) {
            if (requestId > this.maxHandledId) {
                this.maxHandledId = requestId;
            }
        }
    }

    public void onDataAckSent(int ackId) {
        E.checkArgument(this.state == TransportState.ESTABLISHED ||
                        this.state == TransportState.FINISH_RECV,
                        "The state must be ESTABLISHED or FINISH_RECV " +
                        "instead of %s at onDataAckSent()", this.state);
        E.checkArgument(ackId > this.maxAckId,
                        "The ackId must be increasing at onDataAckSent(), " +
                        "ackId: %s, maxAckId: %s", ackId, this.maxAckId);

        this.maxAckId = ackId;
    }

    public boolean needAckFinish() {
        if (this.state == TransportState.FINISH_RECV) {
            return this.maxHandledId == this.finishId - 1;
        }
        return false;
    }

    public boolean needAckData() {
        if (this.state != TransportState.ESTABLISHED) {
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
