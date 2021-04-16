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

import org.junit.Test;

import com.baidu.hugegraph.computer.core.network.TransportStatus;
import com.baidu.hugegraph.testutil.Assert;

public class TransportSessionTest {

    @Test
    public void testConstruct() {
        TransportSession serverSession = new ServerSession();
        Assert.assertEquals(TransportStatus.READY, serverSession.status());

        TransportSession clientSession = new ClientSession();
        Assert.assertEquals(TransportStatus.READY, clientSession.status());
    }

    @Test
    public void testReady() {
        TransportSession serverSession = new ServerSession();
        serverSession.ready();
        Assert.assertEquals(TransportStatus.READY, serverSession.status());
        Assert.assertEquals(-1, serverSession.maxRequestId());
    }

    @Test
    public void testStartSendAndStartRecv() {
        ClientSession clientSession = new ClientSession();
        clientSession.startSend();
        Assert.assertEquals(TransportStatus.START_SEND, clientSession.status);
        ServerSession serverSession = new ServerSession();
        serverSession.startRecv();
        Assert.assertEquals(TransportStatus.START_RECV, serverSession.status);
    }

    @Test
    public void testEstablish() {
        ClientSession clientSession = new ClientSession();
        clientSession.establish();
        Assert.assertEquals(TransportStatus.ESTABLISH, clientSession.status);
        ServerSession serverSession = new ServerSession();
        serverSession.establish();
        Assert.assertEquals(TransportStatus.ESTABLISH, clientSession.status);
    }

    @Test
    public void testFinishSendAndFinishRecv() {
        ClientSession clientSession = new ClientSession();
        clientSession.finishSend();
        Assert.assertEquals(TransportStatus.FINISH_SEND, clientSession.status);
        ServerSession serverSession = new ServerSession();
        serverSession.finishRecv();
        Assert.assertEquals(TransportStatus.FINISH_RECV, serverSession.status);
    }
}
