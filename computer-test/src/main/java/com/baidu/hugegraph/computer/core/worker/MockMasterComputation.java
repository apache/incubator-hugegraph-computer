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

package com.baidu.hugegraph.computer.core.worker;

import com.baidu.hugegraph.computer.core.master.DefaultMasterComputation;
import com.baidu.hugegraph.computer.core.master.MasterContext;
import com.baidu.hugegraph.testutil.Assert;

public class MockMasterComputation extends DefaultMasterComputation {

    @Override
    public boolean compute(MasterContext context) {
        Assert.assertEquals(100L, context.totalVertexCount());
        Assert.assertEquals(200L, context.totalEdgeCount());
        Assert.assertEquals(50L, context.finishedVertexCount());
        Assert.assertEquals(60L, context.messageCount());
        Assert.assertEquals(70L, context.messageBytes());
        return true;
    }
}
