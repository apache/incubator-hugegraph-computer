/*
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

package org.apache.hugegraph.computer.core.graph.value;

import java.io.IOException;

import org.apache.commons.collections.ListUtils;
import org.apache.hugegraph.computer.core.graph.id.BytesId;
import org.apache.hugegraph.computer.core.graph.id.Id;
import org.apache.hugegraph.computer.suite.unit.UnitTestBase;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class IdListListTest extends UnitTestBase {

    @Test
    public void test() {
        Id longId1 = BytesId.of(100L);
        Id longId2 = BytesId.of(200L);

        IdList listValue1 = new IdList();
        IdList listValue2 = new IdList();
        IdListList listListValue1 = new IdListList();
        IdListList listListValue2 = new IdListList();

        listValue1.add(longId1);
        listValue2.add(longId2);
        listListValue1.add(listValue1);
        listListValue2.add(listValue1);

        Assert.assertEquals(ValueType.ID_LIST_LIST,
                            listListValue1.valueType());
        Assert.assertEquals(ValueType.ID_LIST,
                            listListValue1.elemType());
        Assert.assertTrue(ListUtils.isEqualList(
                          Lists.newArrayList(listValue1),
                          listListValue1.values()));
        Assert.assertEquals(listListValue1, listListValue2);

        listValue2.add(longId2);
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
        Id longId1 = BytesId.of(100L);
        Id longId2 = BytesId.of(200L);
        IdList listValue = new IdList();
        listValue.add(longId1);
        listValue.add(longId2);

        IdListList oldValue = new IdListList();
        oldValue.add(listValue);
        assertValueEqualAfterWriteAndRead(oldValue);
    }

    @Test
    public void testCompare() {
        Id longId1 = BytesId.of(100L);
        Id longId2 = BytesId.of(200L);
        IdList listValue = new IdList();
        listValue.add(longId1);
        listValue.add(longId2);

        IdListList value1 = new IdListList();
        value1.add(listValue);
        IdListList value2 = new IdListList();
        value2.add(listValue);
        IdListList value3 = new IdListList();
        value3.add(listValue);
        value3.add(listValue);

        Assert.assertEquals(0, value1.compareTo(value2));
        Assert.assertLt(0, value1.compareTo(value3));
        Assert.assertGt(0, value3.compareTo(value1));
    }
}
