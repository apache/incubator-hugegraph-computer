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

import java.io.IOException;

import org.apache.commons.collections.ListUtils;
import org.junit.Test;

import com.baidu.hugegraph.computer.core.UnitTestBase;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.Lists;

public class IdValueListTest extends UnitTestBase {

    @Test
    public void test() {
        LongId longId1 = new LongId(100L);
        LongId longId2 = new LongId(200L);
        IdValueList listValue1 = new IdValueList();
        IdValueList listValue2 = new IdValueList();

        listValue1.add(longId1.idValue());
        listValue2.add(longId1.idValue());

        Assert.assertEquals(ValueType.ID_VALUE_LIST, listValue1.type());
        Assert.assertEquals(ValueType.ID_VALUE, listValue1.elemType());
        Assert.assertTrue(ListUtils.isEqualList(
                          Lists.newArrayList(longId1.idValue()),
                          listValue1.values()));
        Assert.assertEquals(listValue1, listValue2);

        listValue2.add(longId2.idValue());
        Assert.assertTrue(ListUtils.isEqualList(
                          Lists.newArrayList(longId1.idValue(),
                                             longId2.idValue()),
                          listValue2.values()));
        Assert.assertNotEquals(listValue1, listValue2);
        Assert.assertEquals(ListUtils.hashCodeForList(
                            Lists.newArrayList(longId1.idValue())),
                            listValue1.hashCode());
    }

    @Test
    public void testReadWrite() throws IOException {
        LongId longId1 = new LongId(100L);
        LongId longId2 = new LongId(200L);
        IdValueList oldValue = new IdValueList();
        oldValue.add(longId1.idValue());
        oldValue.add(longId2.idValue());
        assertValueEqualAfterWriteAndRead(oldValue);
    }
}
