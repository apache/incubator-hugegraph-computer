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

package com.baidu.hugegraph.computer.core.input;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.input.hg.HugeInputSplitFetcher;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.HugeClientBuilder;
import com.baidu.hugegraph.testutil.Assert;

public class InputSplitDataTest {

    private static final String URL = "http://127.0.0.1:8080";
    private static final String GRAPH = "hugegraph";

    private static HugeClient client;
    private static MasterInputHandler masterInputHandler;
    private static MockWorkerInputHandler workerInputHandler;

    @BeforeClass
    public static void setup() {
        client = new HugeClientBuilder(URL, GRAPH).build();
        InputSplitFetcher fetcher = new HugeInputSplitFetcher(client);
        masterInputHandler = new MasterInputHandler(fetcher);
        MockRpcClient rpcClient = new MockRpcClient(masterInputHandler);
        workerInputHandler = new MockWorkerInputHandler(rpcClient, client);
    }

    @AfterClass
    public static void teardown() {
        client.close();
    }

    @Test
    public void testMasterCreateAndPollInputSplits() {
        long count = masterInputHandler.createVertexInputSplits();
        Assert.assertGt(0L, count);
        InputSplit split;
        while ((split = masterInputHandler.pollVertexInputSplit()) !=
               InputSplit.END_SPLIT) {
            Assert.assertNotNull(split.start());
            Assert.assertNotNull(split.end());
            count--;
        }
        Assert.assertEquals(InputSplit.END_SPLIT, split);
        Assert.assertEquals(0, count);

        count = masterInputHandler.createEdgeInputSplits();
        Assert.assertGt(0L, count);
        while ((split = masterInputHandler.pollEdgeInputSplit()) !=
               InputSplit.END_SPLIT) {
            Assert.assertNotNull(split.start());
            Assert.assertNotNull(split.end());
            count--;
        }
        Assert.assertEquals(InputSplit.END_SPLIT, split);
        Assert.assertEquals(0, count);
    }

    @Test
    public void testWorkerFetchAndLoadEdgeInputSplits() {
        long count = masterInputHandler.createVertexInputSplits();
        Assert.assertGt(0L, count);
        while (workerInputHandler.fetchNextVertexInputSplit()) {
            workerInputHandler.loadVertexInputSplitData();
            count--;
        }
        Assert.assertEquals(0, count);

        count = masterInputHandler.createEdgeInputSplits();
        Assert.assertGt(0L, count);
        while (workerInputHandler.fetchNextEdgeInputSplit()) {
            workerInputHandler.loadEdgeInputSplitData();
            count--;
        }
        Assert.assertEquals(0, count);
    }
}
