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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.network.TransportState;
import org.apache.hugegraph.computer.core.network.message.AbstractMessage;
import org.apache.hugegraph.computer.core.network.message.MessageType;
import org.apache.hugegraph.computer.core.network.netty.AbstractNetworkTest;
import org.apache.hugegraph.computer.core.util.StringEncodeUtil;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.ExecutorUtil;
import org.apache.hugegraph.util.Log;
import org.junit.Test;
import org.slf4j.Logger;

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

        ClientSession clientSession = new ClientSession(conf, message -> null);
        Assert.assertEquals(TransportState.READY,
                            clientSession.state());
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxRequestId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxAckId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.finishId);
        Assert.assertFalse(clientSession.flowBlocking());
    }

    @Test
    public void testStart() throws TransportException, InterruptedException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> null);
        Assert.assertEquals(TransportState.READY, clientSession.state());

        this.syncStartWithAutoComplete(executorService, clientSession);

        executorService.shutdown();
    }

    @Test
    public void testFinish() throws TransportException, InterruptedException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> null);
        Assert.assertEquals(TransportState.READY, clientSession.state());

        this.syncStartWithAutoComplete(executorService, clientSession);

        int finishId = AbstractMessage.START_SEQ + 1;
        this.syncFinishWithAutoComplete(executorService, clientSession,
                                        finishId);

        executorService.shutdown();
    }

    @Test
    public void testStartWithException() throws InterruptedException,
                                                TransportException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> null);
        Assert.assertEquals(TransportState.READY, clientSession.state());

        this.syncStartWithAutoComplete(executorService, clientSession);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            clientSession.start(conf.timeoutSyncRequest());
        }, e -> {
            Assert.assertContains("The state must be READY " +
                                  "instead of ESTABLISHED at startAsync()",
                                  e.getMessage());
        });
    }

    @Test
    public void testSendAsyncWithException() {
        ClientSession clientSession = new ClientSession(conf, message -> null);
        Assert.assertEquals(TransportState.READY, clientSession.state());

        ByteBuffer buffer = ByteBuffer.wrap(StringEncodeUtil.encode("test data"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            clientSession.sendAsync(MessageType.MSG, 1, buffer);
        }, e -> {
            Assert.assertContains("The state must be ESTABLISHED " +
                                  "instead of READY at sendAsync()",
                                  e.getMessage());
        });
    }

    @Test
    public void testFinishWithException() throws InterruptedException,
                                                 TransportException {
        ScheduledExecutorService executorService =
        ExecutorUtil.newScheduledThreadPool(1, TASK_SCHEDULER);

        ClientSession clientSession = new ClientSession(conf, message -> null);
        Assert.assertEquals(TransportState.READY, clientSession.state());

        this.syncStartWithAutoComplete(executorService, clientSession);

        this.syncFinishWithAutoComplete(executorService, clientSession, 1);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            clientSession.finish(conf.timeoutFinishSession());
        }, e -> {
            Assert.assertContains("The state must be ESTABLISHED " +
                                  "instead of READY at finishAsync()",
                                  e.getMessage());
        });
    }

    @Test
    public void testServerSession() {
        ServerSession serverSession = new ServerSession(conf);
        Assert.assertEquals(TransportState.READY, serverSession.state());

        serverSession.onRecvStateStart();
        Assert.assertEquals(TransportState.START_RECV, serverSession.state());

        serverSession.completeStateStart();
        Assert.assertEquals(TransportState.ESTABLISHED, serverSession.state());
        Assert.assertEquals(AbstractMessage.START_SEQ, serverSession.maxAckId);
        Assert.assertEquals(AbstractMessage.START_SEQ,
                            Whitebox.getInternalState(serverSession,
                                                      "maxHandledId"));

        for (int i = 1; i < 100 ; i++) {
            serverSession.onRecvData(i);
            Assert.assertEquals(i, serverSession.maxRequestId);
            serverSession.onHandledData(i);
            Assert.assertEquals(i, Whitebox.getInternalState(serverSession,
                                                             "maxHandledId"));
            serverSession.onDataAckSent(i);
            Assert.assertEquals(i, serverSession.maxAckId);
        }

        serverSession.onRecvStateFinish(100);
        Assert.assertEquals(TransportState.FINISH_RECV,
                            serverSession.state());

        serverSession.completeStateFinish();
        Assert.assertEquals(TransportState.READY, serverSession.state());
    }

    @Test
    public void testServerSessionWithException() {
        ServerSession serverSession = new ServerSession(conf);
        Assert.assertEquals(TransportState.READY, serverSession.state());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            serverSession.onRecvData(1);
        }, e -> {
            Assert.assertContains("The state must be ESTABLISHED " +
                                  "instead of READY",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            serverSession.onRecvStateFinish(1);
        }, e -> {
            Assert.assertContains("The state must be ESTABLISHED " +
                                  "instead of READY",
                                  e.getMessage());
        });

        serverSession.onRecvStateStart();
        serverSession.completeStateStart();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            serverSession.onRecvData(2);
        }, e -> {
            Assert.assertContains("The requestId must be increasing",
                                  e.getMessage());
        });

        serverSession.onRecvData(1);
        serverSession.onHandledData(1);
        serverSession.onDataAckSent(1);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            serverSession.onDataAckSent(1);
        }, e -> {
            Assert.assertContains("The ackId must be increasing",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            serverSession.onRecvStateFinish(1);
        }, e -> {
            Assert.assertContains("The finishId must be maxRequestId + 1",
                                  e.getMessage());
        });
    }

    @Test
    public void testCheckFinishReady() {
        ServerSession serverSession = new ServerSession(conf);
        Assert.assertEquals(TransportState.READY, serverSession.state());

        serverSession.onRecvStateStart();
        serverSession.completeStateStart();

        Boolean finishRead2 = Whitebox.invoke(serverSession.getClass(),
                                              "needAckFinish",
                                              serverSession);
        Assert.assertFalse(finishRead2);

        serverSession.onRecvStateFinish(1);
        serverSession.completeStateFinish();
    }

    private void syncStartWithAutoComplete(ScheduledExecutorService pool,
                                           ClientSession clientSession)
                                           throws TransportException,
                                                  InterruptedException {
        List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        pool.schedule(() -> {
            Assert.assertEquals(TransportState.START_SENT,
                                clientSession.state());
            try {
                clientSession.onRecvAck(AbstractMessage.START_SEQ);
            } catch (Throwable e) {
                LOG.error("Failed to call receiveAck", e);
                exceptions.add(e);
            }
        }, 2, TimeUnit.SECONDS);

        clientSession.start(conf.timeoutSyncRequest());

        Assert.assertEquals(TransportState.ESTABLISHED,
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
            Assert.assertEquals(TransportState.FINISH_SENT,
                                clientSession.state());
            try {
                clientSession.onRecvAck(finishId);
            } catch (Throwable e) {
                LOG.error("Failed to call receiveAck", e);
                exceptions.add(e);
            }
        }, 2, TimeUnit.SECONDS);

        clientSession.finish(conf.timeoutFinishSession());

        Assert.assertEquals(TransportState.READY,
                            clientSession.state());
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.finishId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxAckId);
        Assert.assertEquals(AbstractMessage.UNKNOWN_SEQ,
                            clientSession.maxRequestId);
        Assert.assertFalse(clientSession.flowBlocking());

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
