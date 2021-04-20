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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.network.TransportStatus;
import com.baidu.hugegraph.computer.core.network.message.AbstractMessage;
import com.baidu.hugegraph.computer.core.network.netty.AbstractNetworkTest;
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
        Assert.assertEquals(TransportStatus.READY,
                            serverSession.status());
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            serverSession.maxRequestId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            serverSession.maxAckId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            serverSession.finishId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            Whitebox.getInternalState(serverSession,
                                                      "maxHandledId"));
        Assert.assertEquals(0L,
                            Whitebox.getInternalState(serverSession,
                                                      "lastAckTimestamp"));

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportStatus.READY,
                            clientSession.status());
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxRequestId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxAckId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.finishId);
        Assert.assertFalse(clientSession.flowControllerStatus());
    }

    @Test
    public void testSyncStart() throws TransportException,
                                       InterruptedException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportStatus.READY, clientSession.status());

        this.syncStartWithAutoComplete(executorService, clientSession,
                                       exceptions);

        Assert.assertFalse(this.existError(exceptions));

        executorService.shutdown();
    }

    @Test
    public void testSyncFinish() throws TransportException,
                                        InterruptedException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportStatus.READY, clientSession.status());

        List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        this.syncStartWithAutoComplete(executorService, clientSession,
                                       exceptions);

        int finishId = AbstractMessage.START_SEQ + 1;
        this.syncFinishWithAutoComplete(executorService, clientSession,
                                        finishId, exceptions);

        Assert.assertFalse(this.existError(exceptions));

        executorService.shutdown();
    }

    @Test
    public void testServerSession() {
        ServerSession serverSession = new ServerSession(conf);
        Assert.assertEquals(TransportStatus.READY, serverSession.status());

        serverSession.startRecv();
        Assert.assertEquals(TransportStatus.START_RECV, serverSession.status());

        serverSession.startComplete();
        Assert.assertEquals(TransportStatus.ESTABLISH, serverSession.status());
        Assert.assertEquals(AbstractMessage.START_SEQ, serverSession.maxAckId);
        Assert.assertEquals(AbstractMessage.START_SEQ,
                            Whitebox.getInternalState(serverSession, "maxHandledId"));

        for (int i = 1; i < 100 ; i++) {
            serverSession.dataRecv(i);
            Assert.assertEquals(i, serverSession.maxRequestId);
            serverSession.handledData(i);
            Assert.assertEquals(i, Whitebox.getInternalState(serverSession,
                                                             "maxHandledId"));

            Whitebox.setInternalState(serverSession,
                                      "lastAckTimestamp", 0L);
            Boolean checkRespondAck = Whitebox.invoke(serverSession.getClass(),
                                                      "checkRespondAck",
                                                      serverSession);
            Assert.assertTrue(checkRespondAck);

            serverSession.respondedAck(i);
            Assert.assertEquals(i, serverSession.maxAckId);
        }

        serverSession.finishRecv(100);
        Assert.assertEquals(TransportStatus.FINISH_RECV, serverSession.status());

        serverSession.finishComplete();
        Assert.assertEquals(TransportStatus.READY, serverSession.status());
    }

    @Test
    public void testServerSessionWithException() {
        ServerSession serverSession = new ServerSession(conf);
        Assert.assertEquals(TransportStatus.READY, serverSession.status());

        Assert.assertThrows(IllegalArgumentException.class, ()->{
            serverSession.dataRecv(1);
        }, e -> {
            Assert.assertContains("The status must be ESTABLISH " +
                                  "instead of READY",
                                  e.getMessage());
        });

        serverSession.startRecv();
        serverSession.startComplete();

        Assert.assertThrows(IllegalArgumentException.class, ()->{
            serverSession.dataRecv(2);
        }, e -> {
            Assert.assertContains("The requestId must be auto-increment",
                                  e.getMessage());
        });

        serverSession.dataRecv(1);
        serverSession.handledData(1);
        serverSession.respondedAck(1);

        Assert.assertThrows(IllegalArgumentException.class, ()->{
            serverSession.respondedAck(1);
        }, e -> {
            Assert.assertContains("The ackId must be increasing",
                                  e.getMessage());
        });
    }

    private void syncStartWithAutoComplete(ScheduledExecutorService pool,
                                           ClientSession clientSession,
                                           List<Throwable> exceptions)
                                           throws TransportException,
                                                  InterruptedException {
        pool.schedule(() -> {
            Assert.assertEquals(TransportStatus.START_SEND,
                                clientSession.status());
            try {
                clientSession.ackRecv(AbstractMessage.START_SEQ);
            }catch (Throwable e) {
                LOG.error("Failed to call receiveAck", e);
                exceptions.add(e);
            }
        }, 2, TimeUnit.SECONDS);

        clientSession.syncStart();

        Assert.assertEquals(TransportStatus.ESTABLISH,
                            clientSession.status());
        Assert.assertEquals(AbstractMessage.START_SEQ,
                            clientSession.maxRequestId);
        Assert.assertEquals(AbstractMessage.START_SEQ,
                            clientSession.maxAckId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.finishId);
    }

    private void syncFinishWithAutoComplete(ScheduledExecutorService pool,
                                            ClientSession clientSession,
                                            int finishId,
                                            List<Throwable> exceptions)
                                            throws InterruptedException,
                                                   TransportException {
        pool.schedule(() -> {
            Assert.assertEquals(TransportStatus.FINISH_SEND,
                                clientSession.status());
            try {
                clientSession.ackRecv(finishId);
            }catch (Throwable e) {
                LOG.error("Failed to call receiveAck", e);
                exceptions.add(e);
            }
        }, 2, TimeUnit.SECONDS);

        clientSession.syncFinish();

        Assert.assertEquals(TransportStatus.READY,
                            clientSession.status());
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.finishId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxAckId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxRequestId);
        Assert.assertFalse(clientSession.flowControllerStatus());
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
