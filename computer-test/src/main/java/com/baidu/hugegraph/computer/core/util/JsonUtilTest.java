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

package com.baidu.hugegraph.computer.core.util;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.graph.partition.PartitionStat;
import com.baidu.hugegraph.testutil.Assert;

public class JsonUtilTest {

    @Test
    public void testJson() {
        PartitionStat partitionStat = new PartitionStat();
        String json = JsonUtil.toJson(partitionStat);
        PartitionStat partitionStat1 = JsonUtil.fromJson(
                                                json, PartitionStat.class);
        Assert.assertEquals(partitionStat, partitionStat1);
    }

    @Test
    public void testNull() {
        String json = JsonUtil.toJson(null);
        PartitionStat partitionStat1 = JsonUtil.fromJson(
                json, PartitionStat.class);
        Assert.assertEquals(null, partitionStat1);
    }

    @Test
    public void testException() {
        Assert.assertThrows(ComputerException.class, () -> {
            JsonUtil.fromJson("abc", PartitionStat.class);
        });
    }
}
