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

package com.baidu.hugegraph.computer.core.receiver;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.computer.core.manager.Manager;
import com.baidu.hugegraph.computer.core.network.ConnectionId;
import com.baidu.hugegraph.computer.core.network.MessageHandler;
import com.baidu.hugegraph.computer.core.network.buffer.ManagedBuffer;
import com.baidu.hugegraph.computer.core.network.message.MessageType;
import com.baidu.hugegraph.computer.core.store.DataFileManager;
import com.baidu.hugegraph.computer.core.worker.WorkerStat;
import com.baidu.hugegraph.util.Log;

public class ReceiveManager implements Manager, MessageHandler {

    private static final Logger LOG = Log.logger(ReceiveManager.class);
    public static final String NAME = "receive";

    private final DataFileManager dataFileManager;

    public ReceiveManager(DataFileManager dataFileManager) {
        this.dataFileManager = dataFileManager;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void channelActive(ConnectionId connectionId) {
        // pass
    }

    @Override
    public void channelInactive(ConnectionId connectionId) {
        // pass
    }

    @Override
    public void exceptionCaught(TransportException cause,
                                ConnectionId connectionId) {
        LOG.warn("Exception caught for connection:{}, root cause:{}",
                 connectionId, cause);
    }

    /*
     * Merge vertices and edges in each partition parallel, and get the
     * workerStat. Be called at the end of input superstep.
     */
    public WorkerStat mergeGraph() {
        // TODO: Merge vertices and edges in each partition parallel
        PartitionStat stat1 = new PartitionStat(0, 100L, 200L,
                                                50L, 60L, 70L);
        WorkerStat workerStat = new WorkerStat();
        workerStat.add(stat1);
        return workerStat;
    }

    @Override
    public void handle(MessageType messageType, int partition,
                       ManagedBuffer buffer) {
        // pass
    }
}
