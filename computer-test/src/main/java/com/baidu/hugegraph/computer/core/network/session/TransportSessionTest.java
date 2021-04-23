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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.network.TransportState;
import com.baidu.hugegraph.computer.core.network.message.AbstractMessage;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.network.netty.AbstractNetworkTest;
import com.baidu.hugegraph.computer.core.util.StringEncoding;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

public class TransportSessionTest extends AbstractNetworkTest {

    private static final Logger LOG = Log.logger(TransportSessionTest.class);

    public static final String TASK_SCHEDULER = "task-scheduler-%d";

    @Override
    protected void initOption() {
        super.updateOption(ComputerOptions.TRANSPORT_SYNC_REQUEST_TIMEOUT,
                           5_000L);
    }

    @Test
    public void testConstruct() {
        ServerSession serverSession = new ServerSession(conf);
        Assert.assertEquals(TransportState.READY,
                            serverSession.state());
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            serverSession.maxRequestId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            serverSession.maxAckId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            serverSession.finishId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            Whitebox.getInternalState(serverSession,
                                                      "maxHandledId"));

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportState.READY,
                            clientSession.state());
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxRequestId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxAckId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.finishId);
        Assert.assertFalse(clientSession.flowControlStatus());
    }

    @Test
    public void testSyncStart() throws TransportException,
                                       InterruptedException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportState.READY, clientSession.state());

        this.syncStartWithAutoComplete(executorService, clientSession);

        executorService.shutdown();
    }

    @Test
    public void testSyncFinish() throws TransportException,
                                        InterruptedException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportState.READY, clientSession.state());

        this.syncStartWithAutoComplete(executorService, clientSession);

        int finishId = AbstractMessage.START_SEQ + 1;
        this.syncFinishWithAutoComplete(executorService, clientSession,
                                        finishId);

        executorService.shutdown();
    }

    @Test
    public void testSyncStartWithException() throws InterruptedException,
                                                    TransportException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportState.READY, clientSession.state());

        this.syncStartWithAutoComplete(executorService, clientSession);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            clientSession.syncStart(conf.syncRequestTimeout());
        }, e -> {
            Assert.assertContains("The state must be READY " +
                                  "instead of ESTABLISH at syncStart()",
                                  e.getMessage());
        });
    }

    @Test
    public void testAsyncSendWithException() throws InterruptedException,
                                                     TransportException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportState.READY, clientSession.state());

        ByteBuffer buffer = ByteBuffer.wrap(StringEncoding.encode("test data"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            clientSession.asyncSend(MessageType.MSG, 1, buffer);
        }, e -> {
            Assert.assertContains("The state must be ESTABLISH " +
                                  "instead of READY at asyncSend()",
                                  e.getMessage());
        });
    }

    @Test
    public void testSyncFinishWithException() throws InterruptedException,
                                                    TransportException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportState.READY, clientSession.state());

        this.syncStartWithAutoComplete(executorService, clientSession);

        this.syncFinishWithAutoComplete(executorService, clientSession, 1);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            clientSession.syncFinish(conf.finishSessionTimeout());
        }, e -> {
            Assert.assertContains("The state must be ESTABLISH " +
                                  "instead of READY at syncFinish()",
                                  e.getMessage());
        });
    }

    @Test
    public void testServerSession() {
        ServerSession serverSession = new ServerSession(conf);
        Assert.assertEquals(TransportState.READY, serverSession.state());

        serverSession.startRecv();
        Assert.assertEquals(TransportState.START_RECV, serverSession.state());

        serverSession.startComplete();
        Assert.assertEquals(TransportState.ESTABLISH, serverSession.state());
        Assert.assertEquals(AbstractMessage.START_SEQ, serverSession.maxAckId);
        Assert.assertEquals(AbstractMessage.START_SEQ,
                            Whitebox.getInternalState(serverSession,
                                                      "maxHandledId"));

        for (int i = 1; i < 100 ; i++) {
            serverSession.dataRecv(i);
            Assert.assertEquals(i, serverSession.maxRequestId);
            serverSession.handledData(i);
            Assert.assertEquals(i, Whitebox.getInternalState(serverSession,
                                                             "maxHandledId"));
            serverSession.respondedDataAck(i);
            Assert.assertEquals(i, serverSession.maxAckId);
        }

        serverSession.finishRecv(100);
        Assert.assertEquals(TransportState.FINISH_RECV,
                            serverSession.state());

        serverSession.finishComplete();
        Assert.assertEquals(TransportState.READY, serverSession.state());
    }

    @Test
    public void testServerSessionWithException() {
        ServerSession serverSession = new ServerSession(conf);
        Assert.assertEquals(TransportState.READY, serverSession.state());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            serverSession.dataRecv(1);
        }, e -> {
            Assert.assertContains("The state must be ESTABLISH " +
                                  "instead of READY",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            serverSession.finishRecv(1);
        }, e -> {
            Assert.assertContains("The state must be ESTABLISH " +
                                  "instead of READY",
                                  e.getMessage());
        });

        serverSession.startRecv();
        serverSession.startComplete();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            serverSession.dataRecv(2);
        }, e -> {
            Assert.assertContains("The requestId must be auto-increment",
                                  e.getMessage());
        });

        serverSession.dataRecv(1);
        serverSession.handledData(1);
        serverSession.respondedDataAck(1);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            serverSession.respondedDataAck(1);
        }, e -> {
            Assert.assertContains("The ackId must be increasing",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            serverSession.finishRecv(1);
        }, e -> {
            Assert.assertContains("The finishId must be maxRequestId + 1",
                                  e.getMessage());
        });
    }

    @Test
    public void testCheckFinishReady() {
        ServerSession serverSession = new ServerSession(conf);
        Assert.assertEquals(TransportState.READY, serverSession.state());

        serverSession.startRecv();
        serverSession.startComplete();

        Boolean finishRead2 = Whitebox.invoke(serverSession.getClass(),
                                              "checkFinishReady",
                                              serverSession);
        Assert.assertFalse(finishRead2);

        serverSession.finishRecv(1);
        serverSession.finishComplete();
    }

    private void syncStartWithAutoComplete(ScheduledExecutorService pool,
                                           ClientSession clientSession)
                                           throws TransportException,
                                                  InterruptedException {
        List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        pool.schedule(() -> {
            Assert.assertEquals(TransportState.START_SEND,
                                clientSession.state());
            try {
                clientSession.ackRecv(AbstractMessage.START_SEQ);
            } catch (Throwable e) {
                LOG.error("Failed to call receiveAck", e);
                exceptions.add(e);
            }
        }, 2, TimeUnit.SECONDS);

        clientSession.syncStart(conf.syncRequestTimeout());

        Assert.assertEquals(TransportState.ESTABLISH,
                            clientSession.state());
        Assert.assertEquals(AbstractMessage.START_SEQ,
                            clientSession.maxRequestId);
        Assert.assertEquals(AbstractMessage.START_SEQ,
                            clientSession.maxAckId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.finishId);

        Assert.assertFalse(this.existError(exceptions));
    }

    private void syncFinishWithAutoComplete(ScheduledExecutorService pool,
                                            ClientSession clientSession,
                                            int finishId)
                                            throws InterruptedException,
                                                   TransportException {
        List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        pool.schedule(() -> {
            Assert.assertEquals(TransportState.FINISH_SEND,
                                clientSession.state());
            try {
                clientSession.ackRecv(finishId);
            } catch (Throwable e) {
                LOG.error("Failed to call receiveAck", e);
                exceptions.add(e);
            }
        }, 2, TimeUnit.SECONDS);

        clientSession.syncFinish(conf.finishSessionTimeout());

        Assert.assertEquals(TransportState.READY,
                            clientSession.state());
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.finishId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxAckId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxRequestId);
        Assert.assertFalse(clientSession.flowControlStatus());

        Assert.assertFalse(this.existError(exceptions));
    }

    private boolean existError(List<Throwable> exceptions) {
        boolean error = false;
        for (Throwable e : exceptions) {
            if (e != null) {
                error = true;
                break;
            }
        }
        return error;
    }
}
