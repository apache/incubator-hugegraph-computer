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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.network.TransportConf;
import org.apache.hugegraph.computer.core.network.TransportState;
import org.apache.hugegraph.computer.core.network.buffer.NetworkBuffer;
import org.apache.hugegraph.computer.core.network.buffer.NioBuffer;
import org.apache.hugegraph.computer.core.network.message.AbstractMessage;
import org.apache.hugegraph.computer.core.network.message.DataMessage;
import org.apache.hugegraph.computer.core.network.message.FinishMessage;
import org.apache.hugegraph.computer.core.network.message.Message;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.network.message.StartMessage;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class ClientSession extends TransportSession {

    private static final Logger LOG = Log.logger(ClientSession.class);

    private final int maxPendingRequests;
    private final int minPendingRequests;

    private final Lock lock;
    private volatile boolean flowBlocking;
    private final AtomicReference<CompletableFuture<Void>> startedFutureRef;
    private final AtomicReference<CompletableFuture<Void>> finishedFutureRef;
    private final Function<Message, Future<Void>> sendFunction;

    public ClientSession(TransportConf conf,
                         Function<Message, Future<Void>> sendFunction) {
        super(conf);
        this.maxPendingRequests = this.conf.maxPendingRequests();
        this.minPendingRequests = this.conf.minPendingRequests();
        this.lock = new ReentrantLock();
        this.flowBlocking = false;
        this.startedFutureRef = new AtomicReference<>();
        this.finishedFutureRef = new AtomicReference<>();
        this.sendFunction = sendFunction;
    }

    @Override
    protected void stateReady() {
        this.flowBlocking = false;
        super.stateReady();
    }

    private void stateStartSent() {
        this.maxRequestId = AbstractMessage.START_SEQ;
        this.state = TransportState.START_SENT;
    }

    private void stateFinishSent(int finishId) {
        this.finishId = finishId;
        this.state = TransportState.FINISH_SENT;
    }

    public synchronized void start(long timeout) throws TransportException {
        CompletableFuture<Void> startFuture = this.startAsync();
        try {
            startFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            this.stateReady();
            if (e instanceof TimeoutException) {
                throw new TransportException(
                          "Timeout(%sms) to wait start-response", timeout);
            } else {
                throw new TransportException("Failed to wait start-response",
                                             e);
            }
        } finally {
            startFuture.cancel(false);
            this.startedFutureRef.compareAndSet(startFuture, null);
        }
    }

    public synchronized CompletableFuture<Void> startAsync() {
        E.checkArgument(this.state == TransportState.READY,
                        "The state must be READY instead of %s " +
                        "at startAsync()", this.state);

        CompletableFuture<Void> startedFuture = new CompletableFuture<>();
        boolean success = this.startedFutureRef.compareAndSet(null,
                                                              startedFuture);
        E.checkArgument(success, "The startedFutureRef value must be null " +
                                 "at startAsync()");

        this.stateStartSent();
        try {
            this.sendFunction.apply(StartMessage.INSTANCE);
        } catch (Throwable e) {
            this.stateReady();
            startedFuture.cancel(false);
            this.startedFutureRef.compareAndSet(startedFuture, null);
            throw e;
        }
        return startedFuture;
    }

    public synchronized void finish(long timeout) throws TransportException {
        CompletableFuture<Void> finishFuture = this.finishAsync();
        try {
            finishFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            this.stateEstablished();
            if (e instanceof TimeoutException) {
                throw new TransportException("Timeout(%sms) to wait finish-response", timeout);
            } else {
                throw new TransportException("Failed to wait finish-response", e);
            }
        } finally {
            this.finishedFutureRef.compareAndSet(finishFuture, null);
        }
    }

    public synchronized CompletableFuture<Void> finishAsync() {
        E.checkArgument(this.state == TransportState.ESTABLISHED,
                        "The state must be ESTABLISHED instead of %s " +
                        "at finishAsync()", this.state);

        CompletableFuture<Void> finishedFuture = new CompletableFuture<>();
        boolean success = this.finishedFutureRef.compareAndSet(null, finishedFuture);
        E.checkArgument(success, "The finishedFutureRef value must be null " +
                                 "at finishAsync()");

        int finishId = this.genFinishId();
        this.stateFinishSent(finishId);
        try {
            FinishMessage finishMessage = new FinishMessage(finishId);
            this.sendFunction.apply(finishMessage);
        } catch (Throwable e) {
            this.stateEstablished();
            finishedFuture.cancel(false);
            this.finishedFutureRef.compareAndSet(finishedFuture, null);
            throw e;
        }

        return finishedFuture;
    }

    public synchronized void sendAsync(MessageType messageType, int partition,
                                       ByteBuffer buffer) {
        E.checkArgument(this.state == TransportState.ESTABLISHED,
                        "The state must be ESTABLISHED instead of %s " +
                        "at sendAsync()", this.state);
        int requestId = this.nextRequestId();

        NetworkBuffer networkBuffer = new NioBuffer(buffer);
        DataMessage dataMessage = new DataMessage(messageType, requestId,
                                                  partition, networkBuffer);

        this.sendFunction.apply(dataMessage);

        this.updateFlowBlocking();
    }

    public void onRecvAck(int ackId) {
        switch (this.state) {
            case START_SENT:
                if (ackId == AbstractMessage.START_SEQ) {
                    this.onRecvStartAck();
                    break;
                }
            case FINISH_SENT:
                if (ackId == this.finishId) {
                    this.onRecvFinishAck();
                } else {
                    this.onRecvDataAck(ackId);
                }
                break;
            case ESTABLISHED:
                this.onRecvDataAck(ackId);
                break;
            default:
                throw new ComputerException("Receive one ack message, but " +
                                            "the state not match, state: %s, " +
                                            "ackId: %s", this.state, ackId);
        }
    }

    private void onRecvStartAck() {
        E.checkArgument(this.state == TransportState.START_SENT,
                        "The state must be START_SENT instead of %s " +
                        "at completeStateStart()", this.state);

        this.maxAckId = AbstractMessage.START_SEQ;

        this.stateEstablished();

        CompletableFuture<Void> startedFuture = this.startedFutureRef.get();
        if (startedFuture != null) {
            if (!startedFuture.isCancelled()) {
                boolean complete = startedFuture.complete(null);
                if (!complete) {
                    LOG.warn("The startedFuture can't be completed");
                }
            }
            this.startedFutureRef.compareAndSet(startedFuture, null);
        }
    }

    private void onRecvFinishAck() {
        E.checkArgument(this.state == TransportState.FINISH_SENT,
                        "The state must be FINISH_SENT instead of %s " +
                        "at completeStateFinish()", this.state);

        this.stateReady();

        CompletableFuture<Void> finishedFuture = this.finishedFutureRef.get();
        if (finishedFuture != null) {
            if (!finishedFuture.isCancelled()) {
                boolean complete = finishedFuture.complete(null);
                if (!complete) {
                    LOG.warn("The finishedFuture can't be completed");
                }
            }
            this.finishedFutureRef.compareAndSet(finishedFuture, null);
        }
    }

    private void onRecvDataAck(int ackId) {
        if (ackId > this.maxAckId) {
            this.maxAckId = ackId;
        }
        this.updateFlowBlocking();
    }

    public boolean flowBlocking() {
        return this.flowBlocking;
    }

    private void updateFlowBlocking() {
        this.lock.lock();
        try {
            int pendingRequests = this.maxRequestId - this.maxAckId;

            if (pendingRequests >= this.maxPendingRequests) {
                this.flowBlocking = true;
            } else if (pendingRequests < this.minPendingRequests) {
                this.flowBlocking = false;
            }
        } finally {
            this.lock.unlock();
        }
    }
}
