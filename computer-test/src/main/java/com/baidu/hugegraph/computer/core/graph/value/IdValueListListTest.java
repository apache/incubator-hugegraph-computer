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

import com.baidu.hugegraph.computer.core.BaseCoreTest;
import com.baidu.hugegraph.computer.core.graph.id.LongId;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.Lists;

public class IdValueListListTest extends BaseCoreTest {

    @Test
    public void test() {
        LongId longId1 = new LongId(100L);
        LongId longId2 = new LongId(200L);

        IdValueList listValue1 = new IdValueList();
        IdValueList listValue2 = new IdValueList();
        IdValueListList listListValue1 = new IdValueListList();
        IdValueListList listListValue2 = new IdValueListList();

        listValue1.add(longId1.idValue());
        listValue2.add(longId2.idValue());
        listListValue1.add(listValue1);
        listListValue2.add(listValue1);

        Assert.assertEquals(ValueType.ID_VALUE_LIST_LIST,
                            listListValue1.type());
        Assert.assertEquals(ValueType.ID_VALUE_LIST,
                            listListValue1.elemType());
        Assert.assertTrue(ListUtils.isEqualList(
                          Lists.newArrayList(listValue1),
                          listListValue1.values()));
        Assert.assertEquals(listListValue1, listListValue2);

        listValue2.add(longId2.idValue());
        listListValue2.add(listValue2);
        Assert.assertTrue(ListUtils.isEqualList(
                          Lists.newArrayList(listValue1, listValue2),
                          listListValue2.values()));
        Assert.assertNotEquals(listListValue1, listListValue2);
        Assert.assertEquals(ListUtils.hashCodeForList(
                            Lists.newArrayList(listValue1)),
                            listListValue1.hashCode());
    }

    @Test
    public void testReadWrite() throws IOException {
        LongId longId1 = new LongId(100L);
        LongId longId2 = new LongId(200L);
        IdValueList listValue = new IdValueList();
        listValue.add(longId1.idValue());
        listValue.add(longId2.idValue());

        IdValueListList oldValue = new IdValueListList();
        oldValue.add(listValue);
        assertValueEqualAfterWriteAndRead(oldValue);
    }
}
