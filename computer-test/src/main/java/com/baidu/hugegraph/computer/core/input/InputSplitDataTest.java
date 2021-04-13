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

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.testutil.Assert;

public class InputSplitDataTest {

    private static MockMasterInputManager masterInputManager;
    private static MockWorkerInputManager workerInputManager;

    @BeforeClass
    public static void setup() {
        Config config = ComputerContext.instance().config();
        masterInputManager = new MockMasterInputManager();
        masterInputManager.init(config);

        MockRpcClient rpcClient = new MockRpcClient(
                                  masterInputManager.handler());
        workerInputManager = new MockWorkerInputManager(rpcClient);
        workerInputManager.init(config);
    }

    @AfterClass
    public static void teardown() {
        masterInputManager.close(ComputerContext.instance().config());
        workerInputManager.close(ComputerContext.instance().config());
    }

    @Test
    public void testMasterCreateAndPollInputSplits() {
        MasterInputHandler masterInputHandler = masterInputManager.handler();
        long count = masterInputHandler.createVertexInputSplits();
        Assert.assertGt(0L, count);
        InputSplit split;
        while (!(split = masterInputHandler.pollVertexInputSplit()).equals(
               InputSplit.END_SPLIT)) {
            Assert.assertNotNull(split.start());
            Assert.assertNotNull(split.end());
            count--;
        }
        Assert.assertEquals(InputSplit.END_SPLIT, split);
        Assert.assertEquals(0, count);

        count = masterInputHandler.createEdgeInputSplits();
        Assert.assertGt(0L, count);
        while (!(split = masterInputHandler.pollEdgeInputSplit()).equals(
               InputSplit.END_SPLIT)) {
            Assert.assertNotNull(split.start());
            Assert.assertNotNull(split.end());
            count--;
        }
        Assert.assertEquals(InputSplit.END_SPLIT, split);
        Assert.assertEquals(0, count);
    }

    @Test
    public void testWorkerFetchAndLoadEdgeInputSplits() {
        MasterInputHandler masterInputHandler = masterInputManager.handler();

        long count = masterInputHandler.createVertexInputSplits();
        Assert.assertGt(0L, count);
        while (workerInputManager.fetchNextVertexInputSplit()) {
            Assert.assertGte(0, workerInputManager.loadVertexInputSplitData());
            count--;
        }
        Assert.assertEquals(0, count);

        count = masterInputHandler.createEdgeInputSplits();
        Assert.assertGt(0L, count);
        while (workerInputManager.fetchNextEdgeInputSplit()) {
            Assert.assertGte(0, workerInputManager.loadEdgeInputSplitData());
            count--;
        }
        Assert.assertEquals(0, count);
    }
}
