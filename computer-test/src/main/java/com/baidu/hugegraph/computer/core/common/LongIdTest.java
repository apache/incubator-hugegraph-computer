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

package com.baidu.hugegraph.computer.core.common;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.testutil.Assert;

public class LongIdTest {

    @Test
    public void test() {
        LongId longId1 = new LongId(-100L);
        LongId longId2 = new LongId();
        LongId longId3 = new LongId(-100L);

        Assert.assertEquals(ValueType.LONG_ID, longId1.type());
        Assert.assertTrue(longId1.compareTo(longId2) < 0);
        Assert.assertTrue(longId2.compareTo(longId1) > 0);
        Assert.assertTrue(longId1.compareTo(longId3) == 0);

        Assert.assertEquals(Long.hashCode(-100L), longId1.hashCode());
        Assert.assertTrue(longId1.equals(new LongId(longId1.value())));
        Assert.assertFalse(longId1.equals(longId2));
    }
}
