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

import com.baidu.hugegraph.computer.core.network.TransportStatus;
import com.baidu.hugegraph.computer.core.network.netty.AbstractNetworkTest;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.ExecutorUtil;
import com.baidu.hugegraph.util.Log;

public class TransportSessionTest extends AbstractNetworkTest {

    private static final Logger LOG = Log.logger(TransportSessionTest.class);

    public static final String TASK_SCHEDULER = "task-scheduler-%d";

    @Override
    protected void initOption() {
        //super.updateOption(ComputerOptions.TRANSPORT_SYNC_REQUEST_TIMEOUT,
        //                   10_000L);
    }

    @Test
    public void testConstruct() {
        ServerSession serverSession = new ServerSession(conf);
        Assert.assertEquals(TransportStatus.READY, serverSession.status());

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportStatus.READY, clientSession.status());
    }

    @Test
    public void testSyncStart() throws InterruptedException {
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
    public void testSyncFinish() throws InterruptedException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> true);
        Assert.assertEquals(TransportStatus.READY, clientSession.status());

        List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        this.syncStartWithAutoComplete(executorService, clientSession,
                                       exceptions);

        int finishId = TransportSession.START_REQUEST_ID + 1;
        this.syncFinishWithAutoComplete(executorService, clientSession,
                                        finishId, exceptions);

        Assert.assertFalse(this.existError(exceptions));

        executorService.shutdown();
    }

    //@Test
    //public void testReady() {
    //    TransportSession serverSession = new ServerSession(conf);
    //    serverSession.ready();
    //    Assert.assertEquals(TransportStatus.READY, serverSession.status());
    //    Assert.assertEquals(-1, serverSession.maxRequestId());
    //}
    //
    //@Test
    //public void testEstablish() {
    //    ClientSession clientSession = new ClientSession();
    //    clientSession.establish();
    //    Assert.assertEquals(TransportStatus.ESTABLISH, clientSession.status);
    //    ServerSession serverSession = new ServerSession();
    //    serverSession.establish();
    //    Assert.assertEquals(TransportStatus.ESTABLISH, clientSession.status);
    //}
    //
    //@Test
    //public void testStartSendAndStartRecv() {
    //    ClientSession clientSession = new ClientSession();
    //    clientSession.startSend();
    //    Assert.assertEquals(TransportStatus.START_SEND, clientSession.status);
    //    ServerSession serverSession = new ServerSession();
    //    serverSession.startRecv();
    //    Assert.assertEquals(TransportStatus.START_RECV, serverSession.status);
    //}
    //
    //@Test
    //public void testFinishSendAndFinishRecv() {
    //    ClientSession clientSession = new ClientSession();
    //    clientSession.finishSend();
    //    Assert.assertEquals(TransportStatus.FINISH_SEND, clientSession.status);
    //    ServerSession serverSession = new ServerSession();
    //    serverSession.finishRecv();
    //    Assert.assertEquals(TransportStatus.FINISH_RECV, serverSession.status);
    //}

    private void syncStartWithAutoComplete(ScheduledExecutorService executorService,
                                           ClientSession clientSession,
                                           List<Throwable> exceptions)
                                           throws InterruptedException {
        executorService.schedule(() -> {
            Assert.assertEquals(TransportStatus.START_SEND,
                                clientSession.status());
            try {
                clientSession.receiveAck(TransportSession.START_REQUEST_ID);
            }catch (Throwable e) {
                LOG.error("Failed to call receiveAck", e);
                exceptions.add(e);
            }
        }, 2, TimeUnit.SECONDS);

        clientSession.syncStart();

        Assert.assertEquals(TransportStatus.ESTABLISH,
                            clientSession.status());
        Assert.assertEquals(TransportSession.START_REQUEST_ID,
                            clientSession.maxRequestId);
        Assert.assertEquals(TransportSession.START_REQUEST_ID,
                            clientSession.maxAckId);
        Assert.assertEquals(TransportSession.SEQ_INIT_VALUE,
                            clientSession.finishId);
    }

    private void syncFinishWithAutoComplete(ScheduledExecutorService executorService,
                                            ClientSession clientSession,
                                            int finishId,
                                            List<Throwable> exceptions)
                                            throws InterruptedException {
        executorService.schedule(() -> {
            Assert.assertEquals(TransportStatus.FINISH_SEND,
                                clientSession.status());
            try {
                clientSession.receiveAck(finishId);
            }catch (Throwable e) {
                LOG.error("Failed to call receiveAck", e);
                exceptions.add(e);
            }
        }, 2, TimeUnit.SECONDS);

        clientSession.syncFinish();

        Assert.assertEquals(TransportStatus.READY,
                            clientSession.status());
        Assert.assertEquals(TransportSession.SEQ_INIT_VALUE,
                            clientSession.finishId);
        Assert.assertEquals(TransportSession.SEQ_INIT_VALUE,
                            clientSession.maxAckId);
        Assert.assertEquals(TransportSession.SEQ_INIT_VALUE,
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
