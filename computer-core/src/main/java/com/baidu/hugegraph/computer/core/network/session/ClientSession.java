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

import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.TransportConf;
import com.baidu.hugegraph.computer.core.network.TransportState;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.buffer.NioManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.AbstractMessage;
import com.baidu.hugegraph.computer.core.network.message.DataMessage;
import com.baidu.hugegraph.computer.core.network.message.FinishMessage;
import com.baidu.hugegraph.computer.core.network.message.Message;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.network.message.StartMessage;
import com.baidu.hugegraph.util.E;
import com.google.common.util.concurrent.SettableFuture;

public class ClientSession extends TransportSession {

    private final int maxPendingRequests;
    private final int minPendingRequests;

    private final Lock lock;
    private volatile boolean flowBlocking;
    private final AtomicReference<SettableFuture<Boolean>> startedFutureRef;
    private final AtomicReference<SettableFuture<Boolean>> finishedFutureRef;
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
        Future<Boolean> startFuture = startAsync();
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
            this.startedFutureRef
                .compareAndSet((SettableFuture<Boolean>) startFuture, null);
        }
    }

    public synchronized Future<Boolean> startAsync() {
        E.checkArgument(this.state == TransportState.READY,
                        "The state must be READY instead of %s " +
                        "at start()", this.state);

        SettableFuture<Boolean> startedFuture = SettableFuture.create();
        this.startedFutureRef.set(startedFuture);

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
        Future<Boolean> finishFuture = finishAsync();
        try {
            finishFuture.get(timeout, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            this.stateEstablished();
            if (e instanceof TimeoutException) {
                throw new TransportException(
                      "Timeout(%sms) to wait finish-response", timeout);
            } else {
                throw new TransportException("Failed to wait finish-response",
                                             e);
            }
        } finally {
            this.finishedFutureRef
                .compareAndSet((SettableFuture<Boolean>) finishFuture, null);
        }
    }

    public synchronized Future<Boolean> finishAsync() {
        E.checkArgument(this.state == TransportState.ESTABLISHED,
                        "The state must be ESTABLISHED instead of %s " +
                        "at finish()", this.state);

        SettableFuture<Boolean> finishedFuture = SettableFuture.create();
        this.finishedFutureRef.set(finishedFuture);

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

        ManagedBuffer managedBuffer = new NioManagedBuffer(buffer);
        DataMessage dataMessage = new DataMessage(messageType, requestId,
                                                  partition, managedBuffer);

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

        SettableFuture<Boolean> settableFuture = this.startedFutureRef.get();
        if (settableFuture != null && !settableFuture.isCancelled()) {
            settableFuture.set(true);
            this.startedFutureRef.compareAndSet(settableFuture, null);
        }
    }

    private void onRecvFinishAck() {
        E.checkArgument(this.state == TransportState.FINISH_SENT,
                        "The state must be FINISH_SENT instead of %s " +
                        "at completeStateFinish()", this.state);

        this.stateReady();

        SettableFuture<Boolean> finishedFuture = this.finishedFutureRef.get();
        if (finishedFuture != null && !finishedFuture.isCancelled()) {
            finishedFuture.set(true);
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
