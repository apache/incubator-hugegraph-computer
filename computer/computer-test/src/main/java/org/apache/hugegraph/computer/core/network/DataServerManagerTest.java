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

package org.apache.hugegraph.computer.core.network;

import java.net.InetSocketAddress;

import org.apache.hugegraph.computer.core.common.exception.ComputerException;
import org.apache.hugegraph.computer.core.common.exception.TransportException;
import org.apache.hugegraph.computer.core.config.ComputerOptions;
import org.apache.hugegraph.computer.core.config.Config;
import org.apache.hugegraph.computer.core.input.WorkerInputManager;
import org.apache.hugegraph.computer.core.manager.Manager;
import org.apache.hugegraph.computer.core.manager.Managers;
import org.apache.hugegraph.computer.core.network.connection.ConnectionManager;
import org.apache.hugegraph.computer.core.network.connection.TransportConnectionManager;
import org.apache.hugegraph.computer.core.receiver.MessageRecvManager;
import org.apache.hugegraph.computer.core.sender.MessageSendManager;
import org.apache.hugegraph.computer.core.snapshot.SnapshotManager;
import org.apache.hugegraph.computer.core.sort.sorting.RecvSortManager;
import org.apache.hugegraph.computer.core.sort.sorting.SortManager;
import org.apache.hugegraph.computer.core.store.FileManager;
import org.apache.hugegraph.computer.core.worker.MockComputation;
import org.apache.hugegraph.computer.core.worker.MockMasterComputation;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class DataServerManagerTest extends UnitTestBase {

    @Test
    public void testCloseManagersAfterPartialInitFailure() {
        UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1"
        );
        Managers managers = new Managers();
        Config config = Mockito.mock(Config.class);
        Manager first = Mockito.mock(Manager.class);
        Manager last = Mockito.mock(Manager.class);
        Mockito.when(first.name()).thenReturn("first");
        Mockito.when(last.name()).thenReturn("last");
        IllegalStateException initFailure =
                new IllegalStateException("init failed");
        Mockito.doThrow(initFailure).when(first).init(config);
        managers.add(first);
        managers.add(new DataServerManager(new TransportConnectionManager(),
                                           Mockito.mock(MessageHandler.class)));
        managers.add(new WorkerInputManager(context(),
                                            Mockito.mock(MessageSendManager.class),
                                            Mockito.mock(SnapshotManager.class)));
        managers.add(last);

        Assert.assertThrows(IllegalStateException.class,
                            () -> managers.initAll(config));
        managers.closeAll(config);
        Mockito.verify(last).close(config);
    }

    @Test
    public void testCloseAllContinuesAfterManagerCloseFailure() {
        Managers managers = new Managers();
        Config config = Mockito.mock(Config.class);
        Manager first = Mockito.mock(Manager.class);
        Manager last = Mockito.mock(Manager.class);
        Mockito.when(first.name()).thenReturn("first");
        Mockito.when(last.name()).thenReturn("last");
        Mockito.doThrow(new IllegalStateException("close failed"))
               .when(first).close(config);
        managers.add(first);
        managers.add(last);

        Assert.assertThrows(ComputerException.class,
                            () -> managers.closeAll(config));
        Mockito.verify(last).close(config);
    }

    @Test
    public void test() {
        Config config = UnitTestBase.updateWithRequiredOptions(
            ComputerOptions.JOB_ID, "local_001",
            ComputerOptions.JOB_WORKERS_COUNT, "1",
            ComputerOptions.BSP_LOG_INTERVAL, "30000",
            ComputerOptions.BSP_MAX_SUPER_STEP, "2",
            ComputerOptions.WORKER_COMPUTATION_CLASS,
            MockComputation.class.getName(),
            ComputerOptions.MASTER_COMPUTATION_CLASS,
            MockMasterComputation.class.getName()
        );
        FileManager fileManager = new FileManager();
        fileManager.init(config);
        SortManager sortManager = new RecvSortManager(context());
        sortManager.init(config);
        MessageRecvManager recvManager = new MessageRecvManager(context(),
                                                                fileManager,
                                                                sortManager);
        SnapshotManager snapshotManager = new SnapshotManager(context(),
                                                              null,
                                                              recvManager,
                                                              null);
        recvManager.init(config);
        ConnectionManager connManager = new TransportConnectionManager();
        DataServerManager serverManager = new DataServerManager(connManager,
                                                                recvManager);
        serverManager.init(config);

        Assert.assertEquals(DataServerManager.NAME, serverManager.name());
        InetSocketAddress address = serverManager.address();
        Assert.assertNotEquals(0, address.getPort());
        ConnectionId connectionId = ConnectionId.parseConnectionId(
                                                 address.getHostName(),
                                                 address.getPort());
        recvManager.onChannelActive(connectionId);
        recvManager.onChannelInactive(connectionId);
        TransportException e = new TransportException("test transport " +
                                                      "exception");
        recvManager.exceptionCaught(e, connectionId);
        serverManager.close(config);
        fileManager.close(config);
        sortManager.close(config);
    }
}
