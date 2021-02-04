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

package com.baidu.hugegraph.computer.core.graph.value;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.testutil.Assert;

public class IdValueTest {

    @Test
    public void testCompare() {
        IdValue value1 = new LongId(123L).idValue();
        IdValue value2 = new LongId(123L).idValue();
        IdValue value3 = new LongId(321L).idValue();
        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value2.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
    }
}
