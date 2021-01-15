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

package com.baidu.hugegraph.computer.core.graph.id;

import java.io.IOException;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.BaseCoreTest;
import com.baidu.hugegraph.computer.core.graph.value.IdValue;
import com.baidu.hugegraph.computer.core.graph.value.ValueType;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.NumericUtil;

public class LongIdTest extends BaseCoreTest {

    @Test
    public void test() {
        LongId longId1 = new LongId(-100L);
        LongId longId2 = new LongId();
        LongId longId3 = new LongId(-100L);

        Assert.assertEquals(IdType.LONG, longId1.type());
        IdValue idValue = longId1.idValue();
        Assert.assertEquals(ValueType.ID_VALUE, idValue.type());
        Assert.assertEquals(longId1, idValue.id());

        Assert.assertEquals(new Long(-100L), longId1.asObject());
        Assert.assertEquals(-100L, longId1.asLong());
        Assert.assertArrayEquals(NumericUtil.longToBytes(-100L),
                                 longId1.asBytes());

        Assert.assertTrue(longId1.compareTo(longId2) < 0);
        Assert.assertTrue(longId2.compareTo(longId1) > 0);
        Assert.assertTrue(longId1.compareTo(longId3) == 0);

        Assert.assertEquals(longId1, longId3);
        Assert.assertNotEquals(longId1, longId2);
        Assert.assertEquals(Long.hashCode(-100L), longId1.hashCode());
        Assert.assertEquals(longId1, new LongId(longId1.asLong()));
    }

    @Test
    public void testReadWrite() throws IOException {
        assertIdEqualAfterWriteAndRead(new LongId(100L));
    }
}
